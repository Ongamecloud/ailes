use compact_str::ToCompactString;
use serde::Serialize;
use std::{collections::HashMap, net::IpAddr, sync::Arc};

#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActivityEvent {
    #[serde(rename = "server:power.start")]
    PowerStart,
    #[serde(rename = "server:power.stop")]
    PowerStop,
    #[serde(rename = "server:power.restart")]
    PowerRestart,
    #[serde(rename = "server:power.kill")]
    PowerKill,

    #[serde(rename = "server:console.command")]
    ConsoleCommand,

    #[serde(rename = "server:sftp.login")]
    SftpLogin,
    #[serde(rename = "server:sftp.write")]
    SftpWrite,
    #[serde(rename = "server:sftp.read")]
    SftpRead,
    #[serde(rename = "server:sftp.create")]
    SftpCreate,
    #[serde(rename = "server:sftp.create-directory")]
    SftpCreateDirectory,
    #[serde(rename = "server:sftp.rename")]
    SftpRename,
    #[serde(rename = "server:sftp.delete")]
    SftpDelete,

    #[serde(rename = "server:file.uploaded")]
    FileUploaded,
    #[serde(rename = "server:file.compress")]
    FileCompress,
    #[serde(rename = "server:file.decompress")]
    FileDecompress,
    #[serde(rename = "server:file.create-directory")]
    FileCreateDirectory,
    #[serde(rename = "server:file.write")]
    FileWrite,
    #[serde(rename = "server:file.copy")]
    FileCopy,
    #[serde(rename = "server:file.delete")]
    FileDelete,
    #[serde(rename = "server:file.rename")]
    FileRename,
}

impl ActivityEvent {
    #[inline]
    pub const fn is_sftp_event(self) -> bool {
        matches!(
            self,
            ActivityEvent::SftpWrite
                | ActivityEvent::SftpRead
                | ActivityEvent::SftpCreate
                | ActivityEvent::SftpCreateDirectory
                | ActivityEvent::SftpRename
                | ActivityEvent::SftpDelete
        )
    }
}

#[derive(Debug, Serialize)]
pub struct ApiActivity {
    user: Option<uuid::Uuid>,
    server: uuid::Uuid,
    event: ActivityEvent,
    metadata: Option<serde_json::Value>,

    ip: Option<compact_str::CompactString>,
    schedule: Option<uuid::Uuid>,
    timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct Activity {
    pub user: Option<uuid::Uuid>,
    pub event: ActivityEvent,
    pub metadata: Option<serde_json::Value>,

    pub ip: Option<IpAddr>,
    pub schedule: Option<uuid::Uuid>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

const CHANNEL_CAPACITY: usize = 5120;

pub struct ActivityManager {
    sender: tokio::sync::mpsc::Sender<Activity>,
    task: tokio::task::JoinHandle<()>,
}

impl ActivityManager {
    pub fn new(server: uuid::Uuid, config: &Arc<crate::config::Config>) -> Self {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<Activity>(CHANNEL_CAPACITY);

        let task = tokio::spawn({
            let config = Arc::clone(config);

            async move {
                let mut leftover: Vec<Activity> = Vec::new();

                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(
                        config.load().system.activity_send_interval,
                    ))
                    .await;

                    let mut batch: Vec<Activity> = std::mem::take(&mut leftover);
                    let drained = receiver.recv_many(&mut batch, CHANNEL_CAPACITY).await;

                    if drained == 0 && batch.is_empty() {
                        break;
                    }

                    let merged = merge_activities(batch);

                    let send_count = config.load().system.activity_send_count;
                    let (to_send, rest) = if merged.len() > send_count {
                        let mut iter = merged.into_iter();
                        let head: Vec<_> = iter.by_ref().take(send_count).collect();
                        (head, iter.collect::<Vec<_>>())
                    } else {
                        (merged, Vec::new())
                    };
                    leftover = rest;

                    if to_send.is_empty() {
                        continue;
                    }

                    let len = to_send.len();

                    if let Err(err) = config
                        .client
                        .send_activity(
                            to_send
                                .into_iter()
                                .map(|activity| ApiActivity {
                                    user: activity.user,
                                    server,
                                    event: activity.event,
                                    metadata: activity.metadata,
                                    ip: activity.ip.map(|ip| ip.to_compact_string()),
                                    schedule: activity.schedule,
                                    timestamp: activity.timestamp,
                                })
                                .collect(),
                        )
                        .await
                    {
                        tracing::error!(
                            server = %server,
                            "failed to send {} activities to remote: {:#?}",
                            len,
                            err
                        );
                    }
                }
            }
        });

        Self { sender, task }
    }

    #[inline]
    pub async fn log_activity(&self, activity: Activity) {
        if let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = self.sender.try_send(activity)
        {
            tracing::warn!("activity channel full, dropping activity");
        }
    }
}

impl Drop for ActivityManager {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn merge_activities(activities: Vec<Activity>) -> Vec<Activity> {
    let mut merged: Vec<Activity> = Vec::with_capacity(activities.len());

    type SftpKey = (ActivityEvent, Option<uuid::Uuid>);
    type UploadKey = (Option<uuid::Uuid>, String);

    let mut sftp_events: HashMap<SftpKey, (Activity, Vec<serde_json::Value>)> = HashMap::new();
    let mut file_upload_events: HashMap<UploadKey, (Activity, Vec<serde_json::Value>)> =
        HashMap::new();

    for activity in activities {
        if activity.event.is_sftp_event()
            && let Some(metadata) = &activity.metadata
            && metadata.get("files").is_some()
        {
            let key = (activity.event, activity.user);
            match sftp_events.get_mut(&key) {
                Some((existing, files_acc)) => {
                    let dt = activity
                        .timestamp
                        .signed_duration_since(existing.timestamp)
                        .num_seconds()
                        .abs();
                    if dt <= 60 {
                        files_acc.extend(extract_files(&activity));
                    } else {
                        let (mut base, files) = sftp_events.remove(&key).unwrap();
                        finalize_files(&mut base, files);
                        merged.push(base);
                        let init = extract_files(&activity);
                        sftp_events.insert(key, (activity, init));
                    }
                }
                None => {
                    let init = extract_files(&activity);
                    sftp_events.insert(key, (activity, init));
                }
            }
            continue;
        }

        if activity.event == ActivityEvent::FileUploaded
            && let Some(metadata) = &activity.metadata
            && metadata.get("files").is_some()
            && let Some(dir_str) = metadata.get("directory").and_then(|d| d.as_str())
        {
            let key = (activity.user, dir_str.to_string());
            match file_upload_events.get_mut(&key) {
                Some((existing, files_acc)) => {
                    let dt = activity
                        .timestamp
                        .signed_duration_since(existing.timestamp)
                        .num_seconds()
                        .abs();
                    if dt <= 60 {
                        files_acc.extend(extract_files(&activity));
                    } else {
                        let (mut base, files) = file_upload_events.remove(&key).unwrap();
                        finalize_files(&mut base, files);
                        merged.push(base);
                        let init = extract_files(&activity);
                        file_upload_events.insert(key, (activity, init));
                    }
                }
                None => {
                    let init = extract_files(&activity);
                    file_upload_events.insert(key, (activity, init));
                }
            }
            continue;
        }

        merged.push(activity);
    }

    for (mut base, files) in sftp_events.into_values() {
        finalize_files(&mut base, files);
        merged.push(base);
    }
    for (mut base, files) in file_upload_events.into_values() {
        finalize_files(&mut base, files);
        merged.push(base);
    }

    merged
}

#[inline]
fn extract_files(activity: &Activity) -> Vec<serde_json::Value> {
    activity
        .metadata
        .as_ref()
        .and_then(|m| m.get("files"))
        .and_then(|f| f.as_array())
        .map(|a| a.to_vec())
        .unwrap_or_default()
}

#[inline]
fn finalize_files(activity: &mut Activity, files: Vec<serde_json::Value>) {
    if files.is_empty() {
        return;
    }
    if let Some(metadata) = &mut activity.metadata
        && let Some(files_field) = metadata.get_mut("files")
    {
        *files_field = serde_json::Value::Array(files);
    }
}
