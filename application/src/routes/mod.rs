use bollard::Docker;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Instant};
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;

pub mod api;
mod download;
mod upload;

#[derive(Debug, ToSchema, Serialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum AppContainerType {
    Official,
    Unknown,
    None,
}

#[derive(Hash, Eq, PartialEq, Clone, Copy)]
pub struct MimeCacheKey {
    pub ino: u64,
    pub dev: u64,
    pub modified: u64,
}

impl From<&std::fs::Metadata> for MimeCacheKey {
    fn from(metadata: &std::fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;

        Self {
            ino: metadata.ino(),
            dev: metadata.dev(),
            modified: metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map_or(0, |duration| duration.as_secs()),
        }
    }
}

impl From<&cap_std::fs::Metadata> for MimeCacheKey {
    fn from(metadata: &cap_std::fs::Metadata) -> Self {
        use cap_std::fs::MetadataExt;

        Self {
            ino: metadata.ino(),
            dev: metadata.dev(),
            modified: metadata
                .modified()
                .ok()
                .and_then(|time| time.into_std().duration_since(std::time::UNIX_EPOCH).ok())
                .map_or(0, |duration| duration.as_secs()),
        }
    }
}

pub struct AppState {
    pub start_time: Instant,
    pub container_type: AppContainerType,
    pub version: String,

    pub config: Arc<crate::config::Config>,
    pub docker: Arc<Docker>,
    pub stats_manager: Arc<crate::stats::StatsManager>,
    pub server_manager: Arc<crate::server::manager::ServerManager>,
    pub backup_manager: Arc<crate::server::backup::manager::BackupManager>,
    pub mime_cache: moka::future::Cache<MimeCacheKey, &'static str>,
}

#[derive(ToSchema, Serialize, Deserialize)]
pub struct ApiError<'a> {
    pub error: &'a str,
}

impl<'a> ApiError<'a> {
    #[inline]
    pub fn new(error: &'a str) -> Self {
        Self { error }
    }

    #[inline]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }
}

pub type State = Arc<AppState>;
pub type GetState = axum::extract::State<State>;

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .nest("/download", download::router(state))
        .nest("/upload", upload::router(state))
        .nest("/api", api::router(state))
        .with_state(state.clone())
}
