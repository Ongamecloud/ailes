use crate::io::{
    abort::{AbortListener, AbortReader},
    counting_reader::AsyncCountingReader,
    hash_reader::{AsyncHashReader, HashReader},
    limited_reader::LimitedReader,
};
use futures::{FutureExt, TryStreamExt};
use sha1::Digest;
use std::{
    io::Write,
    path::Path,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::Mutex;

impl super::manager::BackupManager {
    pub async fn append_transfer_part(
        &self,
        form: reqwest::multipart::Form,
        server: &crate::server::Server,
        uuid: uuid::Uuid,
        bytes_archived: &Arc<AtomicU64>,
        bytes_sent: &Arc<AtomicU64>,
        bytes_total: &Arc<AtomicU64>,
    ) -> reqwest::multipart::Form {
        let backup = match self.find(&server.app_state, uuid).await {
            Ok(Some(backup)) => backup,
            Ok(None) => {
                tracing::warn!(server = %server.uuid, "requested backup {uuid} does not exist");
                return form;
            }
            Err(err) => {
                tracing::error!(server = %server.uuid, "failed to find backup {uuid}: {err:#?}");
                return form;
            }
        };

        if backup.adapter() != super::adapters::BackupAdapter::Wings {
            tracing::warn!(
                server = %server.uuid,
                "backup {uuid} is not a Wings backup and cannot be transferred, skipping"
            );
            return form;
        }

        let file_name = match super::adapters::wings::WingsBackup::get_first_file_name(
            &server.app_state.config,
            uuid,
        )
        .await
        {
            Ok((_, file_name)) => file_name,
            Err(err) => {
                tracing::error!(
                    server = %server.uuid,
                    "failed to get first file name for backup {uuid}: {err}"
                );
                return form;
            }
        };

        let file = match tokio::fs::File::open(&file_name).await {
            Ok(file) => file,
            Err(err) => {
                tracing::error!(
                    server = %server.uuid,
                    "failed to open backup file {}: {err}",
                    file_name.display()
                );
                return form;
            }
        };

        let hasher = Arc::new(Mutex::new(sha2::Sha256::new()));
        let reader = AsyncCountingReader::new_with_bytes_read(file, Arc::clone(bytes_archived));
        let reader = AsyncCountingReader::new_with_bytes_read(reader, Arc::clone(bytes_sent));
        let reader = AsyncHashReader::new_with_hasher(reader, Arc::clone(&hasher)).await;

        let (checksum_sender, checksum_receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            checksum_sender
                .send(hex::encode(hasher.lock().await.finalize_reset()))
                .ok();
        });

        bytes_total.fetch_add(
            tokio::fs::metadata(&file_name)
                .await
                .map(|m| m.len())
                .unwrap_or(0),
            Ordering::Relaxed,
        );

        form.part(
            format!("backup-{uuid}"),
            reqwest::multipart::Part::stream(reqwest::Body::wrap_stream(
                tokio_util::io::ReaderStream::with_capacity(reader, crate::TRANSFER_BUFFER_SIZE),
            ))
            .file_name(
                file_name
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string(),
            )
            .mime_str("backup/wings")
            .expect("failed to set mime type for archive"),
        )
        .part(
            format!("backup-checksum-{uuid}"),
            reqwest::multipart::Part::stream(reqwest::Body::wrap_stream(
                checksum_receiver.into_stream(),
            ))
            .file_name(format!("backup-checksum-{uuid}"))
            .mime_str("text/plain")
            .expect("failed to set mime type for checksum"),
        )
    }
}

pub struct BackupReceiver {
    state: crate::routes::State,
    listener: AbortListener,

    received: Vec<uuid::Uuid>,
    checksum: Option<String>,
}

impl BackupReceiver {
    #[inline]
    pub fn new(state: crate::routes::State, listener: AbortListener) -> Self {
        Self {
            state,
            listener,
            received: Vec::new(),
            checksum: None,
        }
    }

    #[inline]
    pub fn into_received(self) -> Vec<uuid::Uuid> {
        self.received
    }

    pub fn handle_field(
        &mut self,
        runtime: &tokio::runtime::Handle,
        field: axum::extract::multipart::Field<'_>,
    ) -> Result<(), anyhow::Error> {
        tracing::debug!(
            "processing backup field: {}",
            field.name().unwrap_or("unknown")
        );

        let uuid = field
            .name()
            .and_then(|n| n.strip_prefix("backup-"))
            .and_then(|n| uuid::Uuid::from_str(n).ok());

        let uuid = match uuid {
            Some(uuid) => uuid,
            None => {
                if field.name().is_some_and(|n| n.contains("checksum")) {
                    let checksum = match self.checksum.take() {
                        Some(checksum) => checksum,
                        None => {
                            return Err(anyhow::anyhow!(
                                "backup checksum does not match multipart checksum, None to be found"
                            ));
                        }
                    };
                    let expected = runtime.block_on(field.text())?;

                    if checksum != expected {
                        return Err(anyhow::anyhow!(
                            "backup checksum does not match multipart checksum, {expected} != {checksum}"
                        ));
                    }

                    return Ok(());
                }

                tracing::warn!(
                    "invalid backup field name: {}",
                    field.name().unwrap_or("unknown")
                );
                return Ok(());
            }
        };

        let file_name = match field.file_name() {
            Some(name) => name.to_string(),
            None => {
                tracing::warn!("backup field without file name found in transfer archive");
                return Ok(());
            }
        };

        match field.content_type() {
            Some("backup/wings") => {
                if file_name.contains("..") || file_name.contains('/') || file_name.contains('\\') {
                    tracing::warn!("invalid backup file name: {file_name}");
                    return Ok(());
                }

                let file_name =
                    Path::new(&self.state.config.load().system.backup_directory).join(file_name);
                let reader =
                    tokio_util::io::StreamReader::new(field.into_stream().map_err(|err| {
                        std::io::Error::other(format!("failed to read multipart field: {err}"))
                    }));
                let reader = tokio_util::io::SyncIoBridge::new(reader);
                let reader = AbortReader::new(reader, self.listener.clone());
                let reader = LimitedReader::new_with_bytes_per_second(
                    reader,
                    self.state
                        .config
                        .load()
                        .system
                        .transfers
                        .download_limit
                        .as_bytes(),
                );
                let mut reader = HashReader::new_with_hasher(reader, sha2::Sha256::new());

                let mut file = match std::fs::File::create(&file_name) {
                    Ok(file) => file,
                    Err(err) => {
                        tracing::error!(
                            "failed to create backup file {}: {err:#?}",
                            file_name.display()
                        );
                        return Ok(());
                    }
                };

                if let Err(err) = crate::io::copy(&mut reader, &mut file) {
                    tracing::error!(
                        "failed to copy backup file {}: {err:#?}",
                        file_name.display()
                    );
                    return Ok(());
                }

                if let Err(err) = file.flush() {
                    tracing::error!(
                        "failed to flush backup file {}: {err:#?}",
                        file_name.display()
                    );
                    return Ok(());
                }

                self.received.push(uuid);
                self.checksum = Some(hex::encode(reader.finish()));

                tracing::debug!(
                    "backup file {} transferred successfully",
                    file_name.display()
                );
            }
            _ => {
                tracing::warn!(
                    "invalid content type for backup field: {:?}",
                    field.content_type()
                );
            }
        }

        Ok(())
    }
}
