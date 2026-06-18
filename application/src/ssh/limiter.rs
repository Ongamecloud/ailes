use crate::remote::AuthenticationType;
use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicUsize},
};
use tokio::sync::Mutex;

struct Ratelimit {
    password_attempts: usize,
    pubkey_attempts: usize,
    last_attempt: std::time::Instant,
}

impl Default for Ratelimit {
    fn default() -> Self {
        Self {
            password_attempts: 0,
            pubkey_attempts: 0,
            last_attempt: std::time::Instant::now(),
        }
    }
}

pub struct SshLimiter {
    config: Arc<crate::config::Config>,
    ratelimits: Arc<Mutex<HashMap<std::net::IpAddr, Ratelimit>>>,
    user_sessions: Arc<parking_lot::Mutex<HashMap<uuid::Uuid, usize>>>,
    open_handles: Arc<AtomicUsize>,

    task: tokio::task::JoinHandle<()>,
}

impl SshLimiter {
    pub fn new(config: Arc<crate::config::Config>) -> Self {
        let ratelimits = Arc::new(Mutex::new(HashMap::<std::net::IpAddr, Ratelimit>::new()));
        let user_sessions = Arc::new(parking_lot::Mutex::new(HashMap::<uuid::Uuid, usize>::new()));

        let task = tokio::spawn({
            let config = Arc::clone(&config);
            let ratelimits = Arc::clone(&ratelimits);
            let user_sessions = Arc::clone(&user_sessions);

            async move {
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

                    let mut ratelimits = ratelimits.lock().await;
                    let now = std::time::Instant::now();
                    ratelimits.retain(|_, ratelimit| {
                        now.duration_since(ratelimit.last_attempt).as_secs()
                            < config.load().system.sftp.limits.authentication_cooldown
                    });
                    drop(ratelimits);

                    let mut user_sessions = user_sessions.lock();
                    user_sessions.retain(|_, session_count| *session_count > 0);
                }
            }
        });

        Self {
            config,
            ratelimits,
            user_sessions,
            open_handles: Arc::new(AtomicUsize::new(0)),
            task,
        }
    }

    pub async fn check_attempt(
        &self,
        ip: std::net::IpAddr,
        authentication_type: AuthenticationType,
    ) -> Result<(), russh::Error> {
        if self
            .config
            .load()
            .system
            .sftp
            .limits
            .authentication_cooldown
            == 0
        {
            return Ok(());
        }

        let mut ratelimits = self.ratelimits.lock().await;
        let entry = ratelimits.entry(ip).or_default();

        if match authentication_type {
            AuthenticationType::Password => {
                entry.password_attempts += 1;
                entry.last_attempt = std::time::Instant::now();
                entry.password_attempts
                    > self
                        .config
                        .load()
                        .system
                        .sftp
                        .limits
                        .authentication_password_attempts
            }
            AuthenticationType::PublicKey => {
                entry.pubkey_attempts += 1;
                entry.last_attempt = std::time::Instant::now();
                entry.pubkey_attempts
                    > self
                        .config
                        .load()
                        .system
                        .sftp
                        .limits
                        .authentication_pubkey_attempts
            }
        } {
            Err(russh::Error::Disconnect)
        } else {
            Ok(())
        }
    }

    pub async fn finish_attempt(
        &self,
        ip: &std::net::IpAddr,
        authentication_type: AuthenticationType,
    ) {
        if self
            .config
            .load()
            .system
            .sftp
            .limits
            .authentication_cooldown
            == 0
        {
            return;
        }

        let mut ratelimits = self.ratelimits.lock().await;
        if let Some(entry) = ratelimits.get_mut(ip) {
            match authentication_type {
                AuthenticationType::Password => {
                    if entry.password_attempts > 0 {
                        entry.password_attempts -= 1;
                    }
                }
                AuthenticationType::PublicKey => {
                    if entry.pubkey_attempts > 0 {
                        entry.pubkey_attempts -= 1;
                    }
                }
            }
        }
    }

    pub fn increment_sessions(&self, user_uuid: uuid::Uuid) -> Result<(), russh::Error> {
        let mut user_sessions = self.user_sessions.lock();
        let count = user_sessions.entry(user_uuid).or_default();

        if *count
            >= self
                .config
                .load()
                .system
                .sftp
                .limits
                .max_connections_per_user
        {
            Err(russh::Error::Disconnect)
        } else {
            *count += 1;
            Ok(())
        }
    }

    pub fn decrement_sessions(&self, user_uuid: uuid::Uuid) {
        let mut user_sessions = self.user_sessions.lock();
        if let Some(count) = user_sessions.get_mut(&user_uuid)
            && *count > 0
        {
            *count -= 1;
        }
    }

    pub fn open_handle(&self) -> Result<SshLimiterHandleGuard, russh_sftp::server::StatusReply> {
        if self.config.load().system.sftp.limits.max_handles_total == 0 {
            return Ok(SshLimiterHandleGuard(Arc::clone(&self.open_handles)));
        }

        let current = self
            .open_handles
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if current >= self.config.load().system.sftp.limits.max_handles_total {
            self.open_handles
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            Err(
                russh_sftp::server::StatusReply::new(russh_sftp::protocol::StatusCode::Failure)
                    .with_language_tag("en-US")
                    .with_message("Maximum open handles reached."),
            )
        } else {
            Ok(SshLimiterHandleGuard(Arc::clone(&self.open_handles)))
        }
    }
}

impl Drop for SshLimiter {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct SshLimiterHandleGuard(Arc<AtomicUsize>);

impl Drop for SshLimiterHandleGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}
