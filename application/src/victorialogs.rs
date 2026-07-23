use regex::Regex;
use serde::Serialize;
use std::sync::{Arc, OnceLock, RwLock};

fn level_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)\b(trace|debug|dbg|info|ifo|lvl|notice|warn|warning|error|err|critical|crit|fatal|severe)\b").unwrap()
    })
}

fn parse_level(message: &str) -> &'static str {
    let Some(caps) = level_regex().captures(message) else {
        return "info";
    };

    let level = caps.get(1).unwrap().as_str();

    match level.to_ascii_lowercase().as_str() {
        "trace" => "trace",
        "debug" | "dbg" => "debug",
        "info" | "ifo" | "lvl" => "info",
        "notice" => "notice",
        "warn" | "warning" => "warning",
        "error" | "err" => "error",
        "critical" | "crit" => "critical",
        "fatal" | "severe" => "fatal",
        _ => "info",
    }
}

#[derive(Clone, Debug)]
pub struct VictoriaLogsConfig {
    pub enabled: bool,
    pub url: String,
    pub username: String,
    pub password: String,
    pub environment: String,
    pub batch_size: usize,
    pub flush_interval: std::time::Duration,
}

#[derive(Serialize)]
struct LogEntry {
    timestamp: String,
    level: &'static str,
    message: String,
    service: &'static str,
    environment: String,
    hostname: String,
    container_id: String,
    server_uuid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    server_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    extra: Option<std::collections::HashMap<String, serde_json::Value>>,
}

pub struct Client {
    config: VictoriaLogsConfig,
    client: reqwest::Client,
    buffer: tokio::sync::mpsc::Sender<LogEntry>,
    shutdown: tokio::sync::watch::Sender<bool>,
    worker_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    hostname: String,
}

impl Client {
    pub fn new(config: VictoriaLogsConfig) -> Option<Arc<Self>> {
        if !config.enabled {
            return None;
        }

        let hostname = sysinfo::System::host_name().unwrap_or_else(|| "unknown".to_string());

        let mut url = config.url.clone();
        if !url.starts_with("http://") && !url.starts_with("https://") {
            url = format!("http://{url}");
        }
        let mut config = config;
        config.url = url;

        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("failed to build reqwest client");

        let (tx, rx) = tokio::sync::mpsc::channel::<LogEntry>(config.batch_size * 4);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let client = Arc::new(Self {
            config: config.clone(),
            client: http_client,
            buffer: tx,
            shutdown: shutdown_tx,
            worker_handle: tokio::sync::Mutex::new(None),
            hostname,
        });

        let worker_client = Arc::clone(&client);
        let handle = tokio::spawn(Self::worker(worker_client, rx, shutdown_rx));

        // We can't set this in the constructor directly since we need the Arc,
        // so we spawn a task to set it
        let client_clone = Arc::clone(&client);
        tokio::spawn(async move {
            *client_clone.worker_handle.lock().await = Some(handle);
        });

        Some(client)
    }

    async fn worker(
        client: Arc<Self>,
        mut rx: tokio::sync::mpsc::Receiver<LogEntry>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut ticker = tokio::time::interval(client.config.flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut batch = Vec::with_capacity(client.config.batch_size);

        let flush = |batch: &mut Vec<LogEntry>, client: &Arc<Self>| {
            if batch.is_empty() {
                return;
            }

            let entries = std::mem::replace(batch, Vec::with_capacity(client.config.batch_size));
            let client = Arc::clone(client);
            tokio::spawn(async move {
                if let Err(err) = client.send_batch(&entries).await {
                    tracing::error!("[VictoriaLogs] Failed to send batch: {}", err);
                }
            });
        };

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    // Drain remaining entries
                    while let Ok(entry) = rx.try_recv() {
                        batch.push(entry);
                    }
                    flush(&mut batch, &client);
                    return;
                }
                _ = ticker.tick() => {
                    flush(&mut batch, &client);
                }
                entry = rx.recv() => {
                    match entry {
                        Some(entry) => {
                            batch.push(entry);
                            if batch.len() >= client.config.batch_size {
                                flush(&mut batch, &client);
                            }
                        }
                        None => {
                            flush(&mut batch, &client);
                            return;
                        }
                    }
                }
            }
        }
    }

    async fn send_batch(&self, logs: &[LogEntry]) -> Result<(), anyhow::Error> {
        let mut buf = Vec::new();
        for entry in logs {
            if let Ok(data) = serde_json::to_vec(entry) {
                buf.extend_from_slice(&data);
                buf.push(b'\n');
            }
        }

        let url = format!(
            "{}/insert/jsonline?_msg_field=message&_time_field=timestamp&_stream_fields=service,environment,container_id,server_uuid&decolorize_fields=msg,message,log",
            self.config.url
        );

        let mut req = self.client.post(&url)
            .header("Content-Type", "application/stream+json")
            .body(buf);

        if !self.config.username.is_empty() && !self.config.password.is_empty() {
            req = req.basic_auth(&self.config.username, Some(&self.config.password));
        }

        let resp = req.send().await?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            anyhow::bail!("VictoriaLogs returned status {}", status);
        }

        Ok(())
    }

    pub fn log(
        &self,
        container_id: &str,
        server_uuid: &str,
        server_name: &str,
        message: &str,
        extra: Option<std::collections::HashMap<String, serde_json::Value>>,
    ) {
        let entry = LogEntry {
            timestamp: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            level: parse_level(message),
            message: message.to_string(),
            service: "wings",
            environment: self.config.environment.clone(),
            hostname: self.hostname.clone(),
            container_id: container_id.to_string(),
            server_uuid: server_uuid.to_string(),
            server_name: if server_name.is_empty() || server_name == "unknown" {
                None
            } else {
                Some(server_name.to_string())
            },
            extra,
        };

        if let Err(_) = self.buffer.try_send(entry) {
            tracing::warn!(
                "[VictoriaLogs] Buffer full, dropping log for container {}",
                container_id
            );
        }
    }

    pub async fn close(&self) {
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.worker_handle.lock().await.take() {
            let _ = handle.await;
        }
    }
}

static GLOBAL_CLIENT: RwLock<Option<Arc<Client>>> = RwLock::new(None);

pub fn init_global(config: VictoriaLogsConfig) {
    let mut global = GLOBAL_CLIENT.write().unwrap();

    if let Some(old) = global.take() {
        let _ = old.shutdown.send(true);
    }

    if !config.enabled {
        *global = None;
        return;
    }

    *global = Client::new(config);
}

pub fn get_global() -> Option<Arc<Client>> {
    GLOBAL_CLIENT.read().unwrap().clone()
}

pub fn close_global() {
    let client = GLOBAL_CLIENT.write().unwrap().take();
    if let Some(client) = client {
        let _ = client.shutdown.send(true);
    }
}
