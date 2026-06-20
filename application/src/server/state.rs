use compact_str::ToCompactString;
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, Ordering},
};
use utoipa::ToSchema;

#[derive(ToSchema, Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[schema(rename_all = "lowercase")]
#[repr(u8)]
pub enum ServerState {
    #[default]
    Offline,
    Starting,
    Stopping,
    Running,
}

impl ServerState {
    #[inline]
    pub fn to_str(self) -> &'static str {
        match self {
            ServerState::Offline => "offline",
            ServerState::Starting => "starting",
            ServerState::Stopping => "stopping",
            ServerState::Running => "running",
        }
    }
}

pub struct ServerStateLock {
    state: AtomicU8,
    locked: AtomicBool,
    pending_restart: AtomicBool,
    sender: tokio::sync::broadcast::Sender<super::websocket::WebsocketMessage>,
    schedule_manager: Arc<super::schedule::manager::ScheduleManager>,
}

impl ServerStateLock {
    pub fn new(
        sender: tokio::sync::broadcast::Sender<super::websocket::WebsocketMessage>,
        schedule_manager: Arc<super::schedule::manager::ScheduleManager>,
    ) -> Self {
        Self {
            state: AtomicU8::new(0),
            locked: AtomicBool::new(false),
            pending_restart: AtomicBool::new(false),
            sender,
            schedule_manager,
        }
    }

    #[inline]
    pub async fn set_state(&self, state: ServerState) {
        if self.get_state() == state {
            return;
        }

        self.state.store(state as u8, Ordering::SeqCst);
        self.schedule_manager
            .execute_server_state_trigger(state)
            .await;

        self.sender
            .send(
                super::websocket::WebsocketMessage::builder(
                    super::websocket::WebsocketEvent::ServerStatus,
                )
                .arg(state.to_str())
                .build(),
            )
            .unwrap_or_default();
        if (state == ServerState::Offline || state == ServerState::Starting)
            && self.get_pending_restart()
        {
            self.set_pending_restart(false);
        }
    }

    pub fn set_pending_restart(&self, pending: bool) {
        if pending && (self.get_pending_restart() || self.get_state() == ServerState::Offline) {
            return;
        }

        self.pending_restart.store(pending, Ordering::Relaxed);
        self.sender
            .send(
                super::websocket::WebsocketMessage::builder(
                    super::websocket::WebsocketEvent::ServerPendingRestart,
                )
                .arg(pending.to_compact_string())
                .build(),
            )
            .ok();
    }

    #[inline]
    pub fn get_state(&self) -> ServerState {
        match self.state.load(Ordering::SeqCst) {
            0 => ServerState::Offline,
            1 => ServerState::Starting,
            2 => ServerState::Stopping,
            3 => ServerState::Running,
            _ => ServerState::Offline,
        }
    }

    #[inline]
    pub fn get_pending_restart(&self) -> bool {
        self.pending_restart.load(Ordering::Relaxed)
    }

    /// Executes an action with the server state locked.
    /// If the action fails, the state is reverted to the previous state.
    /// Returns `Ok(true)` if the action was executed successfully, `Ok(false)` if the lock was not acquired,
    /// and `Err` if an error occurred during the action execution.
    /// If `aquire_timeout` is `Some`, it will wait for the specified duration to acquire the lock.
    /// If the lock is not acquired within the timeout, it returns `Ok(false)`.
    pub async fn execute_action<
        F: FnOnce(bool) -> Fut,
        Fut: Future<Output = Result<(), anyhow::Error>>,
    >(
        &self,
        state: ServerState,
        action: F,
        aquire_timeout: Option<std::time::Duration>,
    ) -> Result<bool, anyhow::Error> {
        let old_state = self.get_state();

        let mut aquired = false;
        if let Some(timeout) = aquire_timeout {
            let instant = std::time::Instant::now();
            while instant.elapsed() < timeout {
                if !self.locked.load(Ordering::SeqCst) {
                    aquired = true;
                    break;
                }

                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        } else if self.locked.load(Ordering::SeqCst) {
            return Ok(false);
        } else {
            aquired = true;
        }

        if !aquired {
            return Ok(false);
        }

        self.locked.store(true, Ordering::SeqCst);

        self.set_state(state).await;
        if let Err(err) = action(aquired).await {
            tracing::error!("failed to execute power action: {:?}", err);

            self.set_state(old_state).await;
            self.locked.store(false, Ordering::SeqCst);

            Err(err)
        } else {
            self.locked.store(false, Ordering::SeqCst);

            Ok(true)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::Notify;

    fn lock() -> ServerStateLock {
        let state = crate::routes::AppState::mock();
        let schedule_manager = Arc::new(super::super::schedule::manager::ScheduleManager::new(
            state.config.clone(),
        ));
        let (sender, _rx) = tokio::sync::broadcast::channel(16);

        ServerStateLock::new(sender, schedule_manager)
    }

    // ServerStateLock

    #[test]
    fn state_round_trips_through_atomic() {
        tokio_test::block_on(async {
            let lock = lock();
            for state in [
                ServerState::Offline,
                ServerState::Starting,
                ServerState::Stopping,
                ServerState::Running,
            ] {
                lock.set_state(state).await;
                assert_eq!(lock.get_state(), state);
            }
        });
    }

    #[test]
    fn pending_restart_blocked_while_offline() {
        tokio_test::block_on(async {
            let lock = lock();
            lock.set_pending_restart(true);
            assert!(!lock.get_pending_restart());
        });
    }

    #[test]
    fn pending_restart_set_and_cleared_while_active() {
        tokio_test::block_on(async {
            let lock = lock();
            lock.set_state(ServerState::Running).await;
            lock.set_pending_restart(true);
            assert!(lock.get_pending_restart());
            lock.set_pending_restart(false);
            assert!(!lock.get_pending_restart());
        });
    }

    #[test]
    fn entering_offline_clears_pending_restart() {
        tokio_test::block_on(async {
            let lock = lock();
            lock.set_state(ServerState::Running).await;
            lock.set_pending_restart(true);
            lock.set_state(ServerState::Offline).await;
            assert!(!lock.get_pending_restart());
        });
    }

    #[test]
    fn entering_starting_clears_pending_restart() {
        tokio_test::block_on(async {
            let lock = lock();
            lock.set_state(ServerState::Running).await;
            lock.set_pending_restart(true);
            lock.set_state(ServerState::Starting).await;
            assert!(!lock.get_pending_restart());
        });
    }

    #[test]
    fn entering_stopping_keeps_pending_restart() {
        tokio_test::block_on(async {
            let lock = lock();
            lock.set_state(ServerState::Running).await;
            lock.set_pending_restart(true);
            lock.set_state(ServerState::Stopping).await;
            assert!(lock.get_pending_restart());
        });
    }

    #[test]
    fn execute_action_runs_and_sets_state() {
        tokio_test::block_on(async {
            let lock = lock();
            let ran = Arc::new(AtomicBool::new(false));
            let out = {
                let ran = ran.clone();
                lock.execute_action(
                    ServerState::Running,
                    move |_| async move {
                        ran.store(true, Ordering::SeqCst);
                        anyhow::Ok(())
                    },
                    None,
                )
                .await
            };
            assert!(out.unwrap());
            assert!(ran.load(Ordering::SeqCst));
            assert_eq!(lock.get_state(), ServerState::Running);
        });
    }

    #[test]
    fn execute_action_reverts_state_on_error() {
        tokio_test::block_on(async {
            let lock = lock();
            let out = lock
                .execute_action(
                    ServerState::Starting,
                    |_| async move { anyhow::bail!("boom") },
                    None,
                )
                .await;
            assert!(out.is_err());
            assert_eq!(lock.get_state(), ServerState::Offline);
        });
    }

    #[test]
    fn execute_action_releases_lock_after_success() {
        tokio_test::block_on(async {
            let lock = lock();
            lock.execute_action(
                ServerState::Running,
                |_| async move { anyhow::Ok(()) },
                None,
            )
            .await
            .unwrap();
            let out = lock
                .execute_action(
                    ServerState::Stopping,
                    |_| async move { anyhow::Ok(()) },
                    None,
                )
                .await;
            assert!(out.unwrap());
            assert_eq!(lock.get_state(), ServerState::Stopping);
        });
    }

    #[test]
    fn execute_action_releases_lock_after_error() {
        tokio_test::block_on(async {
            let lock = lock();
            let _ = lock
                .execute_action(
                    ServerState::Starting,
                    |_| async move { anyhow::bail!("x") },
                    None,
                )
                .await;
            let out = lock
                .execute_action(
                    ServerState::Running,
                    |_| async move { anyhow::Ok(()) },
                    None,
                )
                .await;
            assert!(out.unwrap());
        });
    }

    #[test]
    fn execute_action_without_timeout_refuses_when_locked() {
        tokio_test::block_on(async {
            let lock = lock();
            let started = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());

            let holder = {
                let started = started.clone();
                let release = release.clone();
                lock.execute_action(
                    ServerState::Running,
                    move |_| async move {
                        started.notify_one();
                        release.notified().await;
                        anyhow::Ok(())
                    },
                    None,
                )
            };
            let contender = {
                let lock = &lock;
                let started = started.clone();
                let release = release.clone();
                async move {
                    started.notified().await;
                    let r = lock
                        .execute_action(
                            ServerState::Stopping,
                            |_| async move { anyhow::Ok(()) },
                            None,
                        )
                        .await;
                    release.notify_one();
                    r
                }
            };

            let (held, contended) = tokio::join!(holder, contender);
            assert!(held.unwrap());
            assert!(!contended.unwrap());
        });
    }

    #[test]
    fn execute_action_with_timeout_refuses_when_lock_stays_held() {
        tokio_test::block_on(async {
            let lock = lock();
            let started = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let ran = Arc::new(AtomicBool::new(false));

            let holder = {
                let started = started.clone();
                let release = release.clone();
                lock.execute_action(
                    ServerState::Running,
                    move |_| async move {
                        started.notify_one();
                        release.notified().await;
                        anyhow::Ok(())
                    },
                    None,
                )
            };
            let contender = {
                let l = &lock;
                let started = started.clone();
                let release = release.clone();
                let ran = ran.clone();
                async move {
                    started.notified().await;
                    let r = l
                        .execute_action(
                            ServerState::Stopping,
                            move |_| async move {
                                ran.store(true, Ordering::SeqCst);
                                anyhow::Ok(())
                            },
                            Some(Duration::from_millis(150)),
                        )
                        .await;
                    release.notify_one();
                    r
                }
            };

            let (held, contended) = tokio::join!(holder, contender);
            assert!(held.unwrap());
            assert_eq!(contended.unwrap(), false);
            assert!(!ran.load(Ordering::SeqCst));
        });
    }
}
