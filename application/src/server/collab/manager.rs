use super::{CollabConflict, CollabError, CollabParticipant, CollabSaved, CollabSyncMeta};
use crate::server::{
    activity::{Activity, ActivityEvent},
    filesystem::virtualfs::VirtualWritableFilesystem,
    websocket::{ServerWebsocketHandler, WebsocketEvent, WebsocketMessage},
};
use base64::Engine;
use compact_str::{CompactString, ToCompactString};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use yrs::{
    Doc, GetString, ReadTxn, StateVector, Text, TextRef, Transact, Update, updates::decoder::Decode,
};

const BASE64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;
const RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

#[derive(Clone, Copy, PartialEq, Eq)]
struct ConflictState {
    hash: Option<blake3::Hash>,
    deleted: bool,
}

impl From<ConflictState> for CollabConflict {
    fn from(state: ConflictState) -> Self {
        Self {
            hash: state.hash.map(|hash| hash.to_hex().to_string()),
            deleted: state.deleted,
        }
    }
}

struct Participant {
    user_uuid: uuid::Uuid,
    user_name: CompactString,
    user_avatar: Option<String>,
    handler: Arc<ServerWebsocketHandler>,
}

struct CollabDoc {
    doc: Doc,
    text: TextRef,
    applied_update_bytes: u64,
    disk_hash: blake3::Hash,
}

impl CollabDoc {
    fn new(content: &str) -> Self {
        let doc = Doc::new();
        let text = doc.get_or_insert_text("content");

        {
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 0, content);
        }

        Self {
            doc,
            text,
            applied_update_bytes: 0,
            disk_hash: blake3::hash(content.as_bytes()),
        }
    }

    fn encode_full_state(&self) -> Vec<u8> {
        self.doc
            .transact()
            .encode_state_as_update_v1(&StateVector::default())
    }

    fn content(&self) -> String {
        self.text.get_string(&self.doc.transact())
    }
}

pub struct CollabSession {
    path: CompactString,
    abs_path: PathBuf,
    filesystem: Arc<dyn VirtualWritableFilesystem>,
    doc: std::sync::Mutex<CollabDoc>,
    dirty: AtomicBool,
    conflict: std::sync::Mutex<Option<ConflictState>>,
    participants: Mutex<HashMap<uuid::Uuid, Participant>>,
    save_lock: Mutex<()>,
}

impl CollabSession {
    async fn broadcast(&self, except: Option<uuid::Uuid>, message: WebsocketMessage) {
        let handlers: Vec<Arc<ServerWebsocketHandler>> = {
            let participants = self.participants.lock().await;
            participants
                .iter()
                .filter(|(connection, _)| Some(**connection) != except)
                .map(|(_, p)| Arc::clone(&p.handler))
                .collect()
        };

        for handler in handlers {
            handler.send_message(message.clone()).await;
        }
    }

    async fn broadcast_conflict(&self, state: Option<ConflictState>) {
        self.broadcast(
            None,
            WebsocketMessage::builder(WebsocketEvent::FileCollabConflict)
                .arg(self.path.clone())
                .structured_arg(state.map(CollabConflict::from))
                .build(),
        )
        .await;
    }

    async fn broadcast_resync(&self) {
        self.broadcast(
            None,
            WebsocketMessage::builder(WebsocketEvent::FileCollabError)
                .arg(self.path.clone())
                .arg("resync")
                .build(),
        )
        .await;
    }

    /// Stores the new conflict state and reports whether it differs from the
    /// previous one, so callers can skip re-broadcasting an unchanged conflict.
    fn set_conflict(&self, state: Option<ConflictState>) -> bool {
        let mut conflict = self.conflict.lock().expect("collab conflict lock poisoned");
        if *conflict != state {
            *conflict = state;
            true
        } else {
            false
        }
    }

    fn current_conflict(&self) -> Option<ConflictState> {
        *self.conflict.lock().expect("collab conflict lock poisoned")
    }

    async fn participants_message(&self) -> WebsocketMessage {
        let participants: Vec<CollabParticipant> = {
            let participants = self.participants.lock().await;
            let mut seen = HashSet::new();
            participants
                .values()
                .filter(|p| seen.insert(p.user_uuid))
                .map(|p| CollabParticipant {
                    user: p.user_uuid,
                    name: p.user_name.clone(),
                    avatar: p.user_avatar.clone(),
                })
                .collect()
        };

        WebsocketMessage::builder(WebsocketEvent::FileCollabParticipants)
            .arg(self.path.clone())
            .structured_arg(participants)
            .build()
    }
}

pub struct CollabManager {
    server: uuid::Uuid,
    config: Arc<crate::config::Config>,
    sessions: Arc<Mutex<HashMap<CompactString, Arc<CollabSession>>>>,
    connections: Mutex<HashMap<uuid::Uuid, HashSet<CompactString>>>,
    pending_updates: Mutex<HashMap<(uuid::Uuid, CompactString), Vec<u8>>>,
    pending_teardowns: Arc<Mutex<HashMap<CompactString, tokio::task::AbortHandle>>>,
}

impl CollabManager {
    pub fn new(server: uuid::Uuid, config: &Arc<crate::config::Config>) -> Self {
        Self {
            server,
            config: Arc::clone(config),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            connections: Mutex::new(HashMap::new()),
            pending_updates: Mutex::new(HashMap::new()),
            pending_teardowns: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn resolve(
        &self,
        server: &crate::server::Server,
        raw_path: &str,
    ) -> Result<(PathBuf, CompactString, Arc<dyn VirtualWritableFilesystem>), CollabError> {
        if !self.config.load().system.file_collaboration.enabled {
            return Err(CollabError::User("collaborative editing is disabled"));
        }

        let parent = Path::new(raw_path)
            .parent()
            .ok_or(CollabError::User("file has no parent"))?;
        let file_name = Path::new(raw_path)
            .file_name()
            .ok_or(CollabError::User("invalid file name"))?;

        let (root, filesystem) = server.filesystem.resolve_writable_fs(server, parent).await;
        if !filesystem.is_primary_server_fs() {
            return Err(CollabError::User(
                "collaborative editing is only available on the primary filesystem",
            ));
        }

        let path = root.join(file_name);
        if server.filesystem.is_ignored(&path, false) {
            return Err(CollabError::User("file not found"));
        }

        let key = match server.filesystem.async_canonicalize(&path).await {
            Ok(key) => key,
            Err(_) => server.filesystem.relative_path(&path),
        };

        Ok((path, key.to_string_lossy().to_compact_string(), filesystem))
    }

    async fn read_content(
        filesystem: &Arc<dyn VirtualWritableFilesystem>,
        path: &Path,
        size_cap: u64,
    ) -> Result<String, CollabError> {
        let metadata = filesystem
            .async_metadata(&path)
            .await
            .map_err(|_| CollabError::User("file not found"))?;
        if !metadata.file_type.is_file() {
            return Err(CollabError::User("file is not a file"));
        }
        if metadata.size > size_cap {
            return Err(CollabError::User(
                "file is too large for collaborative editing",
            ));
        }

        let mut handle = filesystem
            .async_read_file(&path, None)
            .await
            .map_err(|_| CollabError::User("file not found"))?;
        if handle.size > size_cap {
            return Err(CollabError::User(
                "file is too large for collaborative editing",
            ));
        }

        let mut buf = Vec::with_capacity(handle.size as usize);
        handle
            .reader
            .read_to_end(&mut buf)
            .await
            .map_err(|err| CollabError::Internal(err.into()))?;
        if buf.len() as u64 > size_cap {
            return Err(CollabError::User(
                "file is too large for collaborative editing",
            ));
        }

        String::from_utf8(buf).map_err(|_| CollabError::User("file is not editable as text"))
    }

    pub async fn subscribe(
        &self,
        server: &crate::server::Server,
        handler: &Arc<ServerWebsocketHandler>,
        user_uuid: uuid::Uuid,
        user_name: CompactString,
        user_avatar: Option<String>,
        raw_path: &str,
    ) -> Result<(), CollabError> {
        let (path, key, filesystem) = self.resolve(server, raw_path).await?;

        let config = self.config.load();
        let size_cap = config.system.file_collaboration.file_size_cap;
        let max_sessions = config.system.file_collaboration.max_sessions_per_server as usize;
        let max_subscriptions =
            config.system.file_collaboration.max_sessions_per_connection as usize;
        drop(config);

        {
            let connections = self.connections.lock().await;
            if let Some(subscribed) = connections.get(&handler.connection_id)
                && !subscribed.contains(&key)
                && subscribed.len() >= max_subscriptions
            {
                return Err(CollabError::User(
                    "too many collaborative sessions open on this connection",
                ));
            }
        }

        if let Some(abort) = self.pending_teardowns.lock().await.remove(&key) {
            abort.abort();
        }

        let session = {
            let mut sessions = self.sessions.lock().await;

            let session = match sessions.get(&key) {
                Some(session) => {
                    let session = Arc::clone(session);

                    // Only refresh an empty (grace-period) session from disk
                    // here. A session with active participants must not have
                    // its doc swapped without resyncing those clients — the
                    // reconciler handles that case and broadcasts a resync.
                    if session.participants.lock().await.is_empty()
                        && !session.dirty.load(Ordering::Relaxed)
                    {
                        let content = Self::read_content(&filesystem, &path, size_cap).await?;
                        {
                            let mut doc = session.doc.lock().expect("collab doc lock poisoned");
                            if doc.disk_hash != blake3::hash(content.as_bytes()) {
                                *doc = CollabDoc::new(&content);
                            }
                        }
                        session.set_conflict(None);
                    }

                    session
                }
                None => {
                    if sessions.len() >= max_sessions {
                        return Err(CollabError::User(
                            "too many collaborative sessions open on this server",
                        ));
                    }

                    let content = Self::read_content(&filesystem, &path, size_cap).await?;
                    let session = Arc::new(CollabSession {
                        path: key.clone(),
                        abs_path: path.clone(),
                        filesystem: Arc::clone(&filesystem),
                        doc: std::sync::Mutex::new(CollabDoc::new(&content)),
                        dirty: AtomicBool::new(false),
                        conflict: std::sync::Mutex::new(None),
                        participants: Mutex::new(HashMap::new()),
                        save_lock: Mutex::new(()),
                    });

                    sessions.insert(key.clone(), Arc::clone(&session));
                    self.spawn_reconciler(&session);
                    tracing::debug!(
                        server = %self.server,
                        path = %key,
                        "opened collaborative editing session"
                    );

                    session
                }
            };

            // Insert the participant while still holding the sessions lock so a
            // concurrent teardown cannot observe an empty participant map and
            // remove the session between lookup and join.
            session.participants.lock().await.insert(
                handler.connection_id,
                Participant {
                    user_uuid,
                    user_name,
                    user_avatar,
                    handler: Arc::clone(handler),
                },
            );

            session
        };
        self.connections
            .lock()
            .await
            .entry(handler.connection_id)
            .or_default()
            .insert(key.clone());

        let (state, dirty) = {
            let doc = session.doc.lock().expect("collab doc lock poisoned");
            (
                doc.encode_full_state(),
                session.dirty.load(Ordering::Relaxed),
            )
        };
        let conflict = session.current_conflict().map(CollabConflict::from);

        handler
            .send_message(
                WebsocketMessage::builder(WebsocketEvent::FileCollabSync)
                    .arg(key)
                    .arg(BASE64.encode(state))
                    .structured_arg(CollabSyncMeta { dirty, conflict })
                    .build(),
            )
            .await;

        let participants = session.participants_message().await;
        session.broadcast(None, participants).await;

        Ok(())
    }

    /// Watches the file behind a session for external (SFTP, HTTP, …) changes.
    /// The task holds only a weak reference and exits once the session is torn
    /// down. mtime is used as a cheap gate; the content hash against the
    /// session's `disk_hash` is the actual source of truth.
    fn spawn_reconciler(&self, session: &Arc<CollabSession>) {
        let weak = Arc::downgrade(session);
        let config = Arc::clone(&self.config);
        let server = self.server;

        tokio::spawn(async move {
            let mut last_mtime: Option<std::time::SystemTime> = None;
            let mut reported_unreadable = false;

            loop {
                tokio::time::sleep(RECONCILE_INTERVAL).await;

                let Some(session) = weak.upgrade() else { break };

                // Skip the tick while a save or reload holds the lock, so a
                // mid-write file is never misread as an external change.
                let Ok(_save_guard) = session.save_lock.try_lock() else {
                    continue;
                };

                // Heal the dirty flag when the doc has converged back to the
                // on-disk state (undo to baseline, or a no-op update applied
                // after a doc swap).
                if session.dirty.load(Ordering::Relaxed) {
                    let converged = {
                        let doc = session.doc.lock().expect("collab doc lock poisoned");
                        blake3::hash(doc.content().as_bytes()) == doc.disk_hash
                    };
                    if converged {
                        session.dirty.store(false, Ordering::Relaxed);
                    }
                }

                let size_cap = config.load().system.file_collaboration.file_size_cap;

                let content = match session.filesystem.async_metadata(&session.abs_path).await {
                    Ok(metadata) if metadata.file_type.is_file() && metadata.size <= size_cap => {
                        if metadata.modified.is_some() && metadata.modified == last_mtime {
                            continue;
                        }

                        match Self::read_content(&session.filesystem, &session.abs_path, size_cap)
                            .await
                        {
                            Ok(content) => {
                                last_mtime = metadata.modified;
                                Ok(content)
                            }
                            Err(_) => Err(false),
                        }
                    }
                    Ok(metadata) if metadata.file_type.is_file() => Err(false),
                    _ => Err(true),
                };

                match content {
                    Ok(content) => {
                        reported_unreadable = false;

                        let disk_hash = blake3::hash(content.as_bytes());
                        let matches = {
                            let doc = session.doc.lock().expect("collab doc lock poisoned");
                            doc.disk_hash == disk_hash
                        };

                        if matches {
                            // Disk went back to what the session knows (e.g. the
                            // external change was reverted) — clear any conflict.
                            if session.set_conflict(None) {
                                session.broadcast_conflict(None).await;
                            }
                        } else if session.dirty.load(Ordering::Relaxed) {
                            let state = ConflictState {
                                hash: Some(disk_hash),
                                deleted: false,
                            };
                            if session.set_conflict(Some(state)) {
                                tracing::debug!(
                                    server = %server,
                                    path = %session.path,
                                    "collab: file changed on disk while session has unsaved changes"
                                );
                                session.broadcast_conflict(Some(state)).await;
                            }
                        } else {
                            let reloaded = {
                                let mut doc = session.doc.lock().expect("collab doc lock poisoned");
                                // Re-check dirty under the doc lock: apply_update
                                // flips it inside this lock, so no update can land
                                // between the check and the swap.
                                if !session.dirty.load(Ordering::Relaxed)
                                    && doc.disk_hash != disk_hash
                                {
                                    *doc = CollabDoc::new(&content);
                                    true
                                } else {
                                    false
                                }
                            };

                            if reloaded {
                                session.set_conflict(None);
                                tracing::debug!(
                                    server = %server,
                                    path = %session.path,
                                    "collab: reloaded clean session from external file change"
                                );
                                session.broadcast_resync().await;
                            }
                        }
                    }
                    Err(deleted) => {
                        if session.dirty.load(Ordering::Relaxed) {
                            let state = ConflictState {
                                hash: None,
                                deleted,
                            };
                            if session.set_conflict(Some(state)) {
                                session.broadcast_conflict(Some(state)).await;
                            }
                        } else if !reported_unreadable {
                            // Clean session over a file that vanished or became
                            // unreadable: force clients to resubscribe once so the
                            // failure surfaces through the normal subscribe path.
                            reported_unreadable = true;
                            session.broadcast_resync().await;
                        }
                    }
                }
            }
        });
    }

    async fn subscribed_session(
        &self,
        server: &crate::server::Server,
        connection_id: uuid::Uuid,
        raw_path: &str,
    ) -> Result<(CompactString, Arc<CollabSession>), CollabError> {
        let (_, key, _) = self.resolve(server, raw_path).await?;

        if !self
            .connections
            .lock()
            .await
            .get(&connection_id)
            .is_some_and(|subscribed| subscribed.contains(&key))
        {
            return Err(CollabError::User("not subscribed to this file"));
        }

        let session = self
            .sessions
            .lock()
            .await
            .get(&key)
            .map(Arc::clone)
            .ok_or(CollabError::User("not subscribed to this file"))?;

        Ok((key, session))
    }

    pub async fn apply_update(
        &self,
        server: &crate::server::Server,
        connection_id: uuid::Uuid,
        raw_path: &str,
        finished: bool,
        chunk: &str,
    ) -> Result<(), CollabError> {
        let (key, session) = self
            .subscribed_session(server, connection_id, raw_path)
            .await?;

        let size_cap = self.config.load().system.file_collaboration.file_size_cap;

        let chunk = BASE64
            .decode(chunk)
            .map_err(|_| CollabError::User("invalid update encoding"))?;

        let update = {
            let mut pending = self.pending_updates.lock().await;
            let pending_key = (connection_id, key.clone());

            match pending.get_mut(&pending_key) {
                Some(buffer) => {
                    if buffer.len() + chunk.len() > size_cap as usize {
                        pending.remove(&pending_key);
                        return Err(CollabError::User("update is too large"));
                    }
                    buffer.extend_from_slice(&chunk);

                    if !finished {
                        return Ok(());
                    }
                    pending.remove(&pending_key).unwrap_or_default()
                }
                None => {
                    if chunk.len() > size_cap as usize {
                        return Err(CollabError::User("update is too large"));
                    }
                    if !finished {
                        pending.insert(pending_key, chunk);
                        return Ok(());
                    }
                    chunk
                }
            }
        };

        let decoded =
            Update::decode_v1(&update).map_err(|_| CollabError::User("invalid update encoding"))?;

        let needs_resync = {
            let mut guard = session.doc.lock().expect("collab doc lock poisoned");

            let overflow = {
                let doc = &mut *guard;
                let mut txn = doc.doc.transact_mut();
                txn.apply_update(decoded)
                    .map_err(|_| CollabError::User("invalid update"))?;

                doc.text.len(&txn) as u64 > size_cap
            };
            session.dirty.store(true, Ordering::Relaxed);

            if overflow {
                let mut content = guard.content();
                content.truncate(size_cap as usize);
                while !content.is_char_boundary(content.len()) {
                    content.pop();
                }
                *guard = CollabDoc::new(&content);

                true
            } else {
                guard.applied_update_bytes += update.len() as u64;

                if guard.applied_update_bytes > size_cap.saturating_mul(8) {
                    let content = guard.content();
                    *guard = CollabDoc::new(&content);
                    true
                } else {
                    false
                }
            }
        };

        if needs_resync {
            session.broadcast_resync().await;

            return Ok(());
        }

        session
            .broadcast(
                Some(connection_id),
                WebsocketMessage::builder(WebsocketEvent::FileCollabUpdate)
                    .arg(key)
                    .arg(BASE64.encode(&update))
                    .build(),
            )
            .await;

        Ok(())
    }

    pub async fn relay_awareness(
        &self,
        server: &crate::server::Server,
        connection_id: uuid::Uuid,
        raw_path: &str,
        payload: &str,
    ) -> Result<(), CollabError> {
        let (key, session) = self
            .subscribed_session(server, connection_id, raw_path)
            .await?;

        session
            .broadcast(
                Some(connection_id),
                WebsocketMessage::builder(WebsocketEvent::FileCollabAwareness)
                    .arg(key)
                    .arg(payload)
                    .build(),
            )
            .await;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn save(
        &self,
        server: &crate::server::Server,
        connection_id: uuid::Uuid,
        user_uuid: uuid::Uuid,
        user_ip: Option<std::net::IpAddr>,
        raw_path: &str,
        force: bool,
        expected_hash: Option<&str>,
    ) -> Result<(), CollabError> {
        let (key, session) = self
            .subscribed_session(server, connection_id, raw_path)
            .await?;
        let (path, _, filesystem) = self.resolve(server, raw_path).await?;
        let parent = Path::new(raw_path)
            .parent()
            .ok_or(CollabError::User("file has no parent"))?;

        let _save_guard = session.save_lock.lock().await;

        let (content, doc_disk_hash) = {
            let doc = session.doc.lock().expect("collab doc lock poisoned");
            (doc.content(), doc.disk_hash)
        };

        let config = self.config.load();
        let history = &config.system.file_history;
        let history_enabled = history.enabled;
        let history_size_cap = history.file_size_cap;
        let size_cap = config.system.file_collaboration.file_size_cap;
        drop(config);

        let (file_exists, old_content_size) = match filesystem.async_metadata(&path).await {
            Ok(metadata) if metadata.file_type.is_file() => (true, metadata.size as i64),
            Ok(_) => return Err(CollabError::User("file is not a file")),
            Err(_) => (false, 0),
        };

        // Read the current disk content once, shared by the conflict check and
        // the history capture.
        let read_cap = history_size_cap.max(size_cap);
        let old_bytes: Option<Vec<u8>> =
            if file_exists && old_content_size > 0 && old_content_size as u64 <= read_cap {
                match filesystem.async_read_file(&path, None).await {
                    Ok(mut handle) if handle.size <= read_cap => {
                        let mut buf = Vec::with_capacity(handle.size as usize);
                        match handle.reader.read_to_end(&mut buf).await {
                            Ok(_) if buf.len() as u64 <= read_cap => Some(buf),
                            _ => None,
                        }
                    }
                    _ => None,
                }
            } else {
                None
            };

        let current_hash: Option<blake3::Hash> = if !file_exists {
            None
        } else if old_content_size == 0 {
            Some(blake3::hash(b""))
        } else {
            // None here means the file grew beyond the readable cap or could
            // not be read — treated as an external change below.
            old_bytes.as_deref().map(blake3::hash)
        };

        if !current_hash.is_some_and(|hash| hash == doc_disk_hash) {
            // A force save only applies when the client resolved the exact disk
            // state it was shown; if disk moved again in between, re-conflict.
            let force_applies = force
                && expected_hash.is_none_or(|expected| {
                    current_hash.is_some_and(|hash| hash.to_hex().as_str() == expected)
                });

            if !force_applies {
                let state = ConflictState {
                    hash: current_hash,
                    deleted: !file_exists,
                };
                session.set_conflict(Some(state));
                session.broadcast_conflict(Some(state)).await;

                return Ok(());
            }
        }

        if !server
            .filesystem
            .async_allocate_in_path(parent, content.len() as i64 - old_content_size, false)
            .await
        {
            return Err(CollabError::User("failed to allocate space"));
        }

        let captured_before: Option<Vec<u8>> = if history_enabled
            && old_content_size > 0
            && old_content_size as u64 <= history_size_cap
        {
            old_bytes
                .as_ref()
                .filter(|buf| buf.len() as u64 <= history_size_cap)
                .cloned()
        } else {
            None
        };

        let mut file = filesystem.async_create_file(&path).await?;
        file.write_all(content.as_bytes())
            .await
            .map_err(|err| CollabError::Internal(anyhow::anyhow!("failed to write file: {err}")))?;
        file.shutdown()
            .await
            .map_err(|err| CollabError::Internal(anyhow::anyhow!("failed to write file: {err}")))?;

        let mut revision_id = None;
        if history_enabled && content.len() as u64 <= history_size_cap {
            match server
                .diff
                .record_edit(
                    &key,
                    captured_before,
                    content.clone().into_bytes(),
                    Some(user_uuid),
                )
                .await
            {
                Ok(id) => {
                    if id != 0 {
                        revision_id = Some(id);
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        server = %self.server,
                        path = %key,
                        "collab: record_edit failed: {err:#}"
                    );
                }
            }
        }

        {
            let mut doc = session.doc.lock().expect("collab doc lock poisoned");
            doc.disk_hash = blake3::hash(content.as_bytes());
            session
                .dirty
                .store(doc.content() != content, Ordering::Relaxed);
        }
        session.set_conflict(None);

        server.activity.log_activity(Activity {
            event: ActivityEvent::FileWrite,
            user: Some(user_uuid),
            ip: user_ip,
            metadata: Some(serde_json::json!({
                "file": key,
                "revision_id": revision_id,
            })),
            schedule: None,
            timestamp: chrono::Utc::now(),
        });

        session
            .broadcast(
                None,
                WebsocketMessage::builder(WebsocketEvent::FileCollabSaved)
                    .arg(key)
                    .structured_arg(CollabSaved {
                        user: user_uuid,
                        revision_id,
                    })
                    .build(),
            )
            .await;

        Ok(())
    }

    /// Discards the session's in-memory state and reloads it from disk — the
    /// "load theirs" conflict resolution. Applies to every participant.
    pub async fn reload(
        &self,
        server: &crate::server::Server,
        connection_id: uuid::Uuid,
        raw_path: &str,
    ) -> Result<(), CollabError> {
        let (key, session) = self
            .subscribed_session(server, connection_id, raw_path)
            .await?;
        let size_cap = self.config.load().system.file_collaboration.file_size_cap;

        let _save_guard = session.save_lock.lock().await;

        let content = Self::read_content(&session.filesystem, &session.abs_path, size_cap).await?;
        {
            let mut doc = session.doc.lock().expect("collab doc lock poisoned");
            *doc = CollabDoc::new(&content);
        }
        session.dirty.store(false, Ordering::Relaxed);
        session.set_conflict(None);

        tracing::debug!(
            server = %self.server,
            path = %key,
            "collab: session reloaded from disk"
        );

        session.broadcast_resync().await;

        Ok(())
    }

    pub async fn unsubscribe(
        &self,
        server: &crate::server::Server,
        connection_id: uuid::Uuid,
        raw_path: &str,
    ) -> Result<(), CollabError> {
        let (_, key, _) = self.resolve(server, raw_path).await?;
        self.leave(connection_id, &key).await;

        Ok(())
    }

    pub async fn disconnect(&self, connection_id: uuid::Uuid) {
        let subscribed = self.connections.lock().await.remove(&connection_id);
        self.pending_updates
            .lock()
            .await
            .retain(|(connection, _), _| *connection != connection_id);

        if let Some(subscribed) = subscribed {
            for key in subscribed {
                self.leave_session(connection_id, &key).await;
            }
        }
    }

    async fn leave(&self, connection_id: uuid::Uuid, key: &CompactString) {
        if let Some(subscribed) = self.connections.lock().await.get_mut(&connection_id) {
            subscribed.remove(key);
        }
        self.pending_updates
            .lock()
            .await
            .remove(&(connection_id, key.clone()));

        self.leave_session(connection_id, key).await;
    }

    async fn leave_session(&self, connection_id: uuid::Uuid, key: &CompactString) {
        let session = match self.sessions.lock().await.get(key) {
            Some(session) => Arc::clone(session),
            None => return,
        };

        let empty = {
            let mut participants = session.participants.lock().await;
            participants.remove(&connection_id);
            participants.is_empty()
        };

        if empty {
            self.schedule_teardown(key.clone()).await;
        } else {
            let participants = session.participants_message().await;
            session.broadcast(None, participants).await;
        }
    }

    async fn schedule_teardown(&self, key: CompactString) {
        let grace = std::time::Duration::from_secs(
            self.config
                .load()
                .system
                .file_collaboration
                .session_grace_period,
        );

        let task = tokio::spawn({
            let key = key.clone();
            let server = self.server;
            let sessions = Arc::clone(&self.sessions);
            let pending_teardowns = Arc::clone(&self.pending_teardowns);

            async move {
                tokio::time::sleep(grace).await;

                let mut sessions = sessions.lock().await;
                if let Some(session) = sessions.get(&key)
                    && session.participants.lock().await.is_empty()
                {
                    if session.dirty.load(Ordering::Relaxed) {
                        tracing::warn!(
                            server = %server,
                            path = %key,
                            "discarding collaborative editing session with unsaved changes"
                        );
                    }
                    sessions.remove(&key);
                }

                pending_teardowns.lock().await.remove(&key);
            }
        });

        let mut pending_teardowns = self.pending_teardowns.lock().await;
        if let Some(old) = pending_teardowns.insert(key, task.abort_handle()) {
            old.abort();
        }
    }
}
