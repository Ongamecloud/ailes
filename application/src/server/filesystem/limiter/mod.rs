use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

mod btrfs_subvolume;
mod fuse_quota;
mod none;
mod xfs_quota;
mod zfs_dataset;

#[async_trait::async_trait]
pub trait DiskLimiterExt: Send + Sync {
    async fn setup(&self) -> Result<(), std::io::Error>;
    async fn attach(&self) -> Result<(), std::io::Error>;
    async fn destroy(&self) -> Result<(), std::io::Error>;

    async fn startup(&self) -> Result<(), std::io::Error> {
        Ok(())
    }
    async fn shutdown(&self) -> Result<(), std::io::Error> {
        Ok(())
    }

    async fn disk_usage(&self) -> Result<u64, std::io::Error>;
    async fn update_disk_limit(&self, limit: u64) -> Result<(), std::io::Error>;
}

#[derive(ToSchema, Deserialize, Serialize, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub enum DiskLimiterMode {
    #[default]
    None,
    BtrfsSubvolume,
    ZfsDataset,
    XfsQuota,
    FuseQuota,
}

impl DiskLimiterMode {
    pub fn get_limiter<'a>(
        self,
        filesystem: &'a crate::server::filesystem::Filesystem,
    ) -> Box<dyn DiskLimiterExt + 'a> {
        match self {
            DiskLimiterMode::None => Box::new(none::NoneLimiter { filesystem }),
            DiskLimiterMode::BtrfsSubvolume => {
                Box::new(btrfs_subvolume::BtrfsSubvolumeLimiter { filesystem })
            }
            DiskLimiterMode::ZfsDataset => Box::new(zfs_dataset::ZfsDatasetLimiter { filesystem }),
            DiskLimiterMode::XfsQuota => Box::new(xfs_quota::XfsQuotaLimiter { filesystem }),
            DiskLimiterMode::FuseQuota => Box::new(fuse_quota::FuseQuotaLimiter { filesystem }),
        }
    }
}
