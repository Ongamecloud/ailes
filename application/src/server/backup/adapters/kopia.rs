use crate::{
    io::{
        compression::{CompressionLevel, writer::CompressionWriter},
        counting_reader::CountingReader,
    },
    models::DirectoryEntry,
    remote::backups::{KopiaBackupConfiguration, RawServerBackup},
    response::ApiResponse,
    routes::MimeCacheValue,
    server::{
        backup::{Backup, BackupCleanExt, BackupCreateExt, BackupExt, BackupFindExt},
        filesystem::{
            archive::StreamableArchiveFormat,
            cap::FileType,
            encode_mode,
            virtualfs::{
                AsyncFileRead, AsyncReadableFileStream, ByteRange, DirectoryListing,
                DirectoryStreamWalk, DirectoryWalk, FileMetadata, FileRead, IsIgnoredFn,
                VirtualReadableFilesystem,
            },
        },
    },
    utils::{CmpExt, PortablePermissions},
};
use chrono::{Datelike, Timelike};
use compact_str::{CompactString, ToCompactString};
use serde::Deserialize;
use sha2::Digest;
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

const BACKUP_UUID_TAG: &str = "backup-uuid";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KopiaManifest {
    id: String,
    root_entry: KopiaRootEntry,
    #[serde(default)]
    stats: KopiaStats,
}

#[derive(Debug, Deserialize)]
struct KopiaRootEntry {
    #[serde(rename = "obj")]
    obj: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KopiaStats {
    #[serde(default)]
    total_size: u64,
    #[serde(default)]
    file_count: u64,
}

pub struct KopiaBackup {
    uuid: uuid::Uuid,
    root_oid: String,
    manifest_id: String,
    total_size: u64,

    config: Arc<crate::config::Config>,
    config_file: PathBuf,
    remote: Arc<KopiaBackupConfiguration>,
}

impl KopiaBackup {
    fn repository_slug(remote: &KopiaBackupConfiguration) -> String {
        let mut hasher = sha2::Sha256::new();
        hasher.update(remote.url.as_bytes());
        hasher.update([0]);
        hasher.update(remote.username.as_bytes());
        hex::encode(hasher.finalize().get(..8).unwrap_or(&[]))
    }

    fn get_kopia_state_path(config: &crate::config::Config) -> PathBuf {
        Path::new(config.load().system.backup_directory.trim_end_matches('/')).join(".kopia")
    }

    fn get_config_file_path(
        config: &crate::config::Config,
        remote: &KopiaBackupConfiguration,
    ) -> PathBuf {
        Self::get_kopia_state_path(config).join(format!("{}.config", Self::repository_slug(remote)))
    }

    fn get_cache_dir_path(
        config: &crate::config::Config,
        remote: &KopiaBackupConfiguration,
    ) -> PathBuf {
        Self::get_kopia_state_path(config).join(format!("{}.cache", Self::repository_slug(remote)))
    }

    fn get_tokio_command(
        config_file: &Path,
        remote: &KopiaBackupConfiguration,
    ) -> tokio::process::Command {
        let mut command = tokio::process::Command::new("kopia");
        command
            .env("KOPIA_PASSWORD", &remote.password)
            .env("TZ", "UTC")
            .arg("--config-file")
            .arg(config_file);

        command
    }

    fn get_std_command(
        config_file: &Path,
        remote: &KopiaBackupConfiguration,
    ) -> std::process::Command {
        let mut command = std::process::Command::new("kopia");
        command
            .env("KOPIA_PASSWORD", &remote.password)
            .env("TZ", "UTC")
            .arg("--config-file")
            .arg(config_file);

        command
    }

    fn parse_human_bytes(value: &str) -> Option<u64> {
        let value = value.trim();
        let (number, unit) = value.split_once(' ').unwrap_or((value, "B"));

        let number: f64 = number.parse().ok()?;
        let multiplier: f64 = match unit.trim() {
            "B" => 1.0,
            "KB" => 1e3,
            "MB" => 1e6,
            "GB" => 1e9,
            "TB" => 1e12,
            "PB" => 1e15,
            "EB" => 1e18,
            _ => return None,
        };

        Some((number * multiplier) as u64)
    }

    fn extract_parenthesised_bytes(line: &str, label: &str) -> Option<u64> {
        let start = line.find(label)? + label.len();
        let open = line.get(start..)?.find('(')? + start + 1;
        let close = line.get(open..)?.find(')')? + open;
        Self::parse_human_bytes(line.get(open..close)?)
    }

    async fn ensure_connected(
        config_file: &Path,
        cache_dir: &Path,
        remote: &KopiaBackupConfiguration,
    ) -> Result<(), anyhow::Error> {
        let status = Self::get_tokio_command(config_file, remote)
            .arg("repository")
            .arg("status")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await;

        if matches!(status, Ok(status) if status.success()) {
            return Ok(());
        }

        if let Some(parent) = config_file.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::create_dir_all(cache_dir).await?;

        let (username, hostname) = remote
            .username
            .split_once('@')
            .unwrap_or((&remote.username, "wings"));

        let output = Self::get_tokio_command(config_file, remote)
            .arg("repository")
            .arg("connect")
            .arg("server")
            .arg("--url")
            .arg(&remote.url)
            .arg("--server-cert-fingerprint")
            .arg(&remote.fingerprint)
            .arg("--override-username")
            .arg(username)
            .arg("--override-hostname")
            .arg(hostname)
            .arg("--cache-directory")
            .arg(cache_dir)
            .output()
            .await?;

        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "failed to connect to Kopia repository server at {}: {}",
                remote.url,
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        Ok(())
    }

    async fn find_snapshot(
        config_file: &Path,
        remote: &KopiaBackupConfiguration,
        uuid: uuid::Uuid,
    ) -> Result<Option<KopiaManifest>, anyhow::Error> {
        let output = Self::get_tokio_command(config_file, remote)
            .arg("snapshot")
            .arg("list")
            .arg("--json")
            .arg("--all")
            .arg("--tags")
            .arg(format!("{BACKUP_UUID_TAG}:{uuid}"))
            .stderr(std::process::Stdio::null())
            .output()
            .await?;

        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "failed to list Kopia snapshots: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        let mut manifests: Vec<KopiaManifest> = serde_json::from_slice(&output.stdout)?;

        Ok(manifests.pop())
    }
}

#[async_trait::async_trait]
impl BackupFindExt for KopiaBackup {
    async fn exists(state: &crate::routes::State, uuid: uuid::Uuid) -> Result<bool, anyhow::Error> {
        let remote = match state.config.client.backup_kopia_configuration(uuid).await {
            Ok(remote) => remote,
            Err(_) => return Ok(false),
        };

        let config_file = Self::get_config_file_path(&state.config, &remote);
        let cache_dir = Self::get_cache_dir_path(&state.config, &remote);

        if Self::ensure_connected(&config_file, &cache_dir, &remote)
            .await
            .is_err()
        {
            return Ok(false);
        }

        Ok(Self::find_snapshot(&config_file, &remote, uuid)
            .await
            .unwrap_or(None)
            .is_some())
    }

    async fn find(
        state: &crate::routes::State,
        uuid: uuid::Uuid,
    ) -> Result<Option<Backup>, anyhow::Error> {
        let remote = match state.config.client.backup_kopia_configuration(uuid).await {
            Ok(remote) => remote,
            Err(_) => return Ok(None),
        };

        let config_file = Self::get_config_file_path(&state.config, &remote);
        let cache_dir = Self::get_cache_dir_path(&state.config, &remote);

        if Self::ensure_connected(&config_file, &cache_dir, &remote)
            .await
            .is_err()
        {
            return Ok(None);
        }

        let manifest = match Self::find_snapshot(&config_file, &remote, uuid).await? {
            Some(manifest) => manifest,
            None => return Ok(None),
        };

        Ok(Some(Backup::Kopia(KopiaBackup {
            uuid,
            root_oid: manifest.root_entry.obj,
            manifest_id: manifest.id,
            total_size: manifest.stats.total_size,
            config: Arc::clone(&state.config),
            config_file,
            remote: Arc::new(remote),
        })))
    }
}

#[async_trait::async_trait]
impl BackupCreateExt for KopiaBackup {
    async fn create(
        server: &crate::server::Server,
        uuid: uuid::Uuid,
        progress: Arc<AtomicU64>,
        total: Arc<AtomicU64>,
        ignore: ignore::gitignore::Gitignore,
        ignore_raw: compact_str::CompactString,
    ) -> Result<RawServerBackup, anyhow::Error> {
        let remote = server
            .app_state
            .config
            .client
            .backup_kopia_configuration(uuid)
            .await?;

        let config_file = Self::get_config_file_path(&server.app_state.config, &remote);
        let cache_dir = Self::get_cache_dir_path(&server.app_state.config, &remote);
        Self::ensure_connected(&config_file, &cache_dir, &remote).await?;

        let source_path = server.filesystem.base_path.clone();

        let total_task = {
            let total = Arc::clone(&total);
            let server = server.clone();
            let ignore = ignore.clone();

            async move {
                let mut walker = server
                    .filesystem
                    .async_walk_dir(Path::new(""))
                    .await?
                    .with_is_ignored(ignore.into());
                let mut total_files = 0;
                while let Some(Ok((_, path))) = walker.next_entry().await {
                    let metadata = match server.filesystem.async_symlink_metadata(&path).await {
                        Ok(metadata) => metadata,
                        Err(_) => continue,
                    };
                    total.fetch_add(metadata.len(), Ordering::Relaxed);
                    if !metadata.is_dir() {
                        total_files += 1;
                    }
                }

                Ok::<_, anyhow::Error>(total_files)
            }
        };

        let ignore_lines: Vec<&str> = ignore_raw
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .collect();
        if !ignore_lines.is_empty() {
            let mut policy = Self::get_tokio_command(&config_file, &remote);
            policy.arg("policy").arg("set").arg(&source_path);
            for line in &ignore_lines {
                policy.arg("--add-ignore").arg(line);
            }

            if let Ok(output) = policy.output().await
                && !output.status.success()
            {
                tracing::warn!(
                    "failed to apply ignore policy for Kopia backup {}: {}",
                    uuid,
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }

        let snapshot_task = {
            let config_file = config_file.clone();
            let remote = remote.clone();
            let source_path = source_path.clone();
            let progress = Arc::clone(&progress);

            async move {
                let mut command = Self::get_tokio_command(&config_file, &remote);
                command
                    .arg("--progress")
                    .arg("--progress-update-interval")
                    .arg("1s")
                    .arg("--progress-estimation-type")
                    .arg("rough")
                    .arg("snapshot")
                    .arg("create")
                    .arg(&source_path)
                    .arg("--json")
                    .arg("--description")
                    .arg(format!("wings backup {uuid}"));

                command
                    .arg("--tags")
                    .arg(format!("{BACKUP_UUID_TAG}:{uuid}"));
                for (key, value) in &remote.tags {
                    command.arg("--tags").arg(format!("{key}:{value}"));
                }

                let mut child = command
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .spawn()?;

                let progress_task = {
                    let progress = Arc::clone(&progress);
                    let stderr = child.stderr.take();

                    async move {
                        let Some(stderr) = stderr else {
                            return;
                        };

                        let mut segments = tokio::io::BufReader::new(stderr).split(b'\r');
                        while let Ok(Some(segment)) = segments.next_segment().await {
                            let line = String::from_utf8_lossy(&segment);

                            fn extract_trailing_bytes(line: &str, label: &str) -> Option<u64> {
                                let start = line.find(label)? + label.len();
                                let rest = line.get(start..)?;
                                let end = rest.find(',').unwrap_or(rest.len());
                                KopiaBackup::parse_human_bytes(rest.get(..end)?)
                            }

                            let hashed = Self::extract_parenthesised_bytes(&line, "hashed ");
                            let cached = Self::extract_parenthesised_bytes(&line, "cached ");
                            let uploaded = extract_trailing_bytes(&line, "uploaded ");

                            progress.store(
                                (hashed.unwrap_or(0) + cached.unwrap_or(0))
                                    .max(uploaded.unwrap_or(0)),
                                Ordering::SeqCst,
                            );
                        }
                    }
                };

                let (output, ()) = tokio::join!(child.wait_with_output(), progress_task);
                let output = output?;
                if !output.status.success() {
                    return Err(anyhow::anyhow!(
                        "failed to create Kopia snapshot: {}",
                        String::from_utf8_lossy(&output.stderr)
                    ));
                }

                let manifest: KopiaManifest = serde_json::from_slice(&output.stdout)?;
                Ok::<_, anyhow::Error>(manifest)
            }
        };

        let (total_files, manifest) = tokio::join!(total_task, snapshot_task);
        let total_files = total_files?;
        let manifest = manifest?;

        progress.store(total.load(Ordering::SeqCst), Ordering::SeqCst);

        let size = if manifest.stats.total_size > 0 {
            manifest.stats.total_size
        } else {
            total.load(Ordering::SeqCst)
        };
        let files = if manifest.stats.file_count > 0 {
            manifest.stats.file_count
        } else {
            total_files
        };

        Ok(RawServerBackup {
            checksum: manifest.id,
            checksum_type: "kopia".into(),
            size,
            files,
            successful: true,
            browsable: true,
            streaming: true,
            parts: vec![],
        })
    }
}

struct KopiaDirectoryEntry {
    path: PathBuf,
    file_type: FileType,
    mode: u32,
    size: u64,
    mtime: chrono::DateTime<chrono::Utc>,
    oid: String,
}

#[async_trait::async_trait]
impl BackupExt for KopiaBackup {
    #[inline]
    fn uuid(&self) -> uuid::Uuid {
        self.uuid
    }

    async fn download(
        &self,
        state: &crate::routes::State,
        archive_format: StreamableArchiveFormat,
        _range: Option<ByteRange>,
    ) -> Result<ApiResponse, anyhow::Error> {
        let compression_level = state.config.load().system.backups.compression_level;
        let file_compression_threads = state.config.load().api.file_compression_threads;
        let (reader, writer) = tokio::io::simplex(crate::BUFFER_SIZE);

        let spawn_restore = || {
            tokio::task::block_in_place(|| {
                Self::get_std_command(&self.config_file, &self.remote)
                    .arg("restore")
                    .arg(&self.root_oid)
                    .arg("/dev/stdout")
                    .arg("--mode")
                    .arg("tar")
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::null())
                    .spawn()
            })
        };

        match archive_format {
            StreamableArchiveFormat::Zip => {
                let child = spawn_restore()?;

                crate::spawn_blocking_handled(move || -> Result<(), anyhow::Error> {
                    let writer = tokio_util::io::SyncIoBridge::new(writer);
                    let mut archive = zip::ZipWriter::new_stream(writer);

                    let stdout = child
                        .stdout
                        .ok_or_else(|| anyhow::anyhow!("kopia restore produced no stdout"))?;
                    let mut subtar = tar::Archive::new(stdout);
                    let mut entries = subtar.entries()?;

                    let mut read_buffer = vec![0; crate::BUFFER_SIZE];
                    while let Some(Ok(mut entry)) = entries.next() {
                        let header = entry.header().clone();
                        let relative = entry.path()?;

                        let is_dir = header.entry_type() == tar::EntryType::Directory;
                        let mode = header.mode().unwrap_or(if is_dir { 0o755 } else { 0o644 });
                        let size = header.size().unwrap_or(0);

                        let mut options: zip::write::FileOptions<'_, ()> =
                            zip::write::FileOptions::default()
                                .compression_level(
                                    Some(compression_level.to_deflate_level() as i64),
                                )
                                .unix_permissions(mode)
                                .large_file(size >= u32::MAX as u64);
                        if let Ok(mtime) = header.mtime()
                            && let Some(mtime) = chrono::DateTime::from_timestamp(mtime as i64, 0)
                        {
                            options =
                                options.last_modified_time(zip::DateTime::from_date_and_time(
                                    mtime.year() as u16,
                                    mtime.month() as u8,
                                    mtime.day() as u8,
                                    mtime.hour() as u8,
                                    mtime.minute() as u8,
                                    mtime.second() as u8,
                                )?);
                        }

                        match header.entry_type() {
                            tar::EntryType::Directory => {
                                archive.add_directory(relative.to_string_lossy(), options)?;
                            }
                            tar::EntryType::Regular => {
                                archive.start_file(relative.to_string_lossy(), options)?;
                                crate::io::copy_shared(&mut read_buffer, &mut entry, &mut archive)?;
                            }
                            _ => continue,
                        }
                    }

                    let mut inner = archive.finish()?.into_inner();
                    inner.flush()?;
                    inner.shutdown()?;

                    Ok(())
                });
            }
            f if f.is_tar() => {
                let child = spawn_restore()?;

                crate::spawn_blocking_handled(move || -> Result<(), anyhow::Error> {
                    let mut writer = CompressionWriter::new(
                        tokio_util::io::SyncIoBridge::new(writer),
                        f.compression_format(),
                        compression_level,
                        file_compression_threads,
                    )?;

                    let mut stdout = child
                        .stdout
                        .ok_or_else(|| anyhow::anyhow!("kopia restore produced no stdout"))?;
                    if let Err(err) = crate::io::copy(&mut stdout, &mut writer) {
                        tracing::error!("failed to compress tar archive for kopia backup: {}", err);
                    }

                    let mut inner = writer.finish()?;
                    inner.flush()?;
                    inner.shutdown()?;

                    Ok(())
                });
            }
            _ => {
                tracing::error!(
                    "unsupported archive format for kopia backup download: {}",
                    archive_format.extension()
                );
            }
        }

        Ok(ApiResponse::new_stream(reader)
            .with_header(
                "Content-Disposition",
                &format!(
                    "attachment; filename={}.{}",
                    self.uuid,
                    archive_format.extension()
                ),
            )
            .with_header("Content-Type", archive_format.mime_type()))
    }

    async fn restore(
        &self,
        server: &crate::server::Server,
        progress: Arc<AtomicU64>,
        total: Arc<AtomicU64>,
        _download_url: Option<compact_str::CompactString>,
    ) -> Result<(), anyhow::Error> {
        total.store(self.total_size, Ordering::SeqCst);

        let mut child = Self::get_tokio_command(&self.config_file, &self.remote)
            .arg("--progress")
            .arg("--progress-update-interval")
            .arg("1s")
            .arg("restore")
            .arg(&self.root_oid)
            .arg(&server.filesystem.base_path)
            .arg("--overwrite-files")
            .arg("--overwrite-directories")
            .arg("--overwrite-symlinks")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()?;

        if let Some(stderr) = child.stderr.take() {
            let mut segments = tokio::io::BufReader::new(stderr).split(b'\r');
            while let Ok(Some(segment)) = segments.next_segment().await {
                let line = compact_str::CompactString::from_utf8_lossy(segment.trim_ascii());
                if line.is_empty() {
                    continue;
                }

                let Some(restored) = Self::extract_parenthesised_bytes(&line, "Processed ") else {
                    continue;
                };

                progress.store(restored, Ordering::SeqCst);
                if let Some(enqueued) = Self::extract_parenthesised_bytes(&line, " of ") {
                    total.store(enqueued, Ordering::SeqCst);
                }
                server.log_daemon(line);
            }
        }

        let status = child.wait().await?;
        if !status.success() {
            return Err(anyhow::anyhow!("failed to restore Kopia backup"));
        }

        progress.store(total.load(Ordering::SeqCst), Ordering::SeqCst);
        server.filesystem.rerun_disk_checker();

        Ok(())
    }

    async fn delete(&self, state: &crate::routes::State) -> Result<(), anyhow::Error> {
        let output = Self::get_tokio_command(&self.config_file, &self.remote)
            .arg("snapshot")
            .arg("delete")
            .arg(&self.manifest_id)
            .arg("--delete")
            .output()
            .await?;

        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "failed to delete Kopia backup: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        state
            .backup_manager
            .invalidate_cached_browse(self.uuid)
            .await;

        Ok(())
    }

    async fn browse(
        &self,
        server: &crate::server::Server,
    ) -> Result<Arc<dyn VirtualReadableFilesystem>, anyhow::Error> {
        let cache_dir = Self::get_cache_dir_path(&self.config, &self.remote);
        Self::ensure_connected(&self.config_file, &cache_dir, &self.remote).await?;

        let mut child = Self::get_tokio_command(&self.config_file, &self.remote)
            .arg("ls")
            .arg("-l")
            .arg("-r")
            .arg("--no-error-summary")
            .arg(&self.root_oid)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;

        let mut entries = Vec::new();

        if let Some(stdout) = child.stdout.take() {
            let mut line_reader = tokio::io::BufReader::new(stdout).lines();

            while let Ok(Some(line)) = line_reader.next_line().await {
                if line.is_empty() {
                    continue;
                }

                fn parse_ls_line(line: &str) -> Option<KopiaDirectoryEntry> {
                    let mut parts = line.split_whitespace();
                    let mode_str = parts.next()?;
                    let size: u64 = parts.next()?.parse().unwrap_or(0);
                    let date = parts.next()?;
                    let time = parts.next()?;
                    let _tz = parts.next()?;
                    let oid = parts.next()?.to_string();

                    let mut raw_path = line.trim_start();
                    for _ in 0..6 {
                        let end = raw_path.find(char::is_whitespace)?;
                        raw_path = raw_path.get(end..)?.trim_start();
                    }
                    let path = raw_path.trim_end_matches('/');
                    if path.is_empty() {
                        return None;
                    }

                    let file_type = match mode_str.chars().next() {
                        Some('d') => FileType::Dir,
                        Some('L') | Some('l') => FileType::Symlink,
                        _ => FileType::File,
                    };

                    let mode = u32::from_str_radix(
                        &mode_str
                            .chars()
                            .skip(1)
                            .map(|c| if c == '-' { '0' } else { '1' })
                            .collect::<String>(),
                        2,
                    )
                    .unwrap_or(if file_type.is_dir() { 0o755 } else { 0o644 });

                    let mtime = chrono::NaiveDateTime::parse_from_str(
                        &format!("{date} {time}"),
                        "%Y-%m-%d %H:%M:%S",
                    )
                    .map(|naive| naive.and_utc())
                    .unwrap_or_else(|_| chrono::DateTime::from_timestamp(0, 0).unwrap_or_default());

                    Some(KopiaDirectoryEntry {
                        path: PathBuf::from(path),
                        file_type,
                        mode,
                        size,
                        mtime,
                        oid,
                    })
                }

                if let Some(entry) = parse_ls_line(&line) {
                    entries.push(entry);
                }
            }
        }

        let status = child.wait().await?;
        if !status.success()
            && let Some(mut stderr) = child.stderr.take()
        {
            let mut stderr_out = String::new();
            stderr.read_to_string(&mut stderr_out).await?;

            tracing::error!(
                "failed to list Kopia snapshot for browsing: {}",
                stderr_out.trim()
            );
        }

        let tree = tokio::task::block_in_place(|| KopiaTreeNode::build(entries));

        Ok(Arc::new(VirtualKopiaBackup {
            server: server.clone(),
            config_file: self.config_file.clone(),
            remote: Arc::clone(&self.remote),
            tree: Arc::new(tree),
        }))
    }
}

#[async_trait::async_trait]
impl BackupCleanExt for KopiaBackup {
    async fn clean(
        _server: &crate::server::Server,
        _uuid: uuid::Uuid,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

struct KopiaFileMeta {
    file_type: FileType,
    mode: u32,
    size: u64,
    mtime: chrono::DateTime<chrono::Utc>,
    oid: String,
}

#[derive(Default)]
struct KopiaTreeNode {
    size: u64,
    mtime: chrono::DateTime<chrono::Utc>,
    mode: u32,
    has_explicit_entry: bool,
    dirs: Vec<(CompactString, KopiaTreeNode)>,
    files: Vec<(CompactString, KopiaFileMeta)>,
}

impl KopiaTreeNode {
    fn build(entries: Vec<KopiaDirectoryEntry>) -> Self {
        let mut root = KopiaTreeNode::default();

        for entry in entries {
            root.insert(entry);
        }
        root.sort_files();
        root.aggregate_sizes();
        root
    }

    fn insert(&mut self, entry: KopiaDirectoryEntry) {
        let components: Vec<&str> = entry
            .path
            .components()
            .filter_map(|c| c.as_os_str().to_str())
            .collect();
        if components.is_empty() {
            return;
        }

        match entry.file_type {
            FileType::Dir => {
                let node = self.upsert_dir_path(&components);
                node.has_explicit_entry = true;
                node.mtime = entry.mtime;
                node.mode = entry.mode;
            }
            _ => {
                let (leaf, parents) = match components.split_last() {
                    Some(value) => value,
                    None => return,
                };

                let parent = self.upsert_dir_path(parents);
                let meta = KopiaFileMeta {
                    file_type: entry.file_type,
                    mode: entry.mode,
                    size: entry.size,
                    mtime: entry.mtime,
                    oid: entry.oid,
                };

                parent.files.push((leaf.to_compact_string(), meta));
            }
        }
    }

    fn sort_files(&mut self) {
        self.files.reverse();
        self.files.sort_by(|(a, _), (b, _)| a.cmp(b));
        self.files.dedup_by(|(a, _), (b, _)| a == b);
        for (_, child) in self.dirs.iter_mut() {
            child.sort_files();
        }
    }

    fn upsert_dir_path(&mut self, components: &[&str]) -> &mut KopiaTreeNode {
        let mut current = self;
        for name in components {
            let idx = match current.dirs.binary_search_by(|(n, _)| n.as_str().cmp(name)) {
                Ok(idx) => idx,
                Err(idx) => {
                    current
                        .dirs
                        .insert(idx, (name.to_compact_string(), KopiaTreeNode::default()));
                    idx
                }
            };
            // SAFETY: `idx` is a valid index into `current.dirs` by construction above.
            current = unsafe { &mut current.dirs.get_unchecked_mut(idx).1 };
        }
        current
    }

    fn aggregate_sizes(&mut self) -> u64 {
        let mut total: u64 = self.files.iter().map(|(_, m)| m.size).sum();
        for (_, child) in self.dirs.iter_mut() {
            total = total.saturating_add(child.aggregate_sizes());
        }
        self.size = total;
        total
    }

    fn lookup_dir(&self, path: &Path) -> Option<&KopiaTreeNode> {
        if path == Path::new("") || path == Path::new("/") {
            return Some(self);
        }
        let mut current = self;
        for component in path.components() {
            let name = component.as_os_str().to_str()?;
            let idx = current
                .dirs
                .binary_search_by(|(n, _)| n.as_str().cmp(name))
                .ok()?;
            current = &current.dirs.get(idx)?.1;
        }
        Some(current)
    }

    fn lookup_file(&self, path: &Path) -> Option<&KopiaFileMeta> {
        let parent_path = path.parent()?;
        let leaf = path.file_name()?.to_str()?;
        let parent = self.lookup_dir(parent_path)?;
        let idx = parent
            .files
            .binary_search_by(|(n, _)| n.as_str().cmp(leaf))
            .ok()?;
        Some(&parent.files.get(idx)?.1)
    }
}

struct SubtreeEntry {
    relative: PathBuf,
    file_type: FileType,
    mode: u32,
    mtime: chrono::DateTime<chrono::Utc>,
    size: u64,
    oid: String,
}

pub struct VirtualKopiaBackup {
    server: crate::server::Server,
    config_file: PathBuf,
    remote: Arc<KopiaBackupConfiguration>,
    tree: Arc<KopiaTreeNode>,
}

impl VirtualKopiaBackup {
    fn open_object(&self, oid: &str) -> Result<std::process::ChildStdout, anyhow::Error> {
        let child = KopiaBackup::get_std_command(&self.config_file, &self.remote)
            .arg("show")
            .arg(oid)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()?;

        child
            .stdout
            .ok_or_else(|| anyhow::anyhow!("kopia show produced no stdout"))
    }

    fn directory_entry_from_dir_node(path: &Path, node: &KopiaTreeNode) -> DirectoryEntry {
        let mode = if node.mode != 0 { node.mode } else { 0o755 };

        DirectoryEntry {
            name: path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into(),
            mode: encode_mode(mode),
            mode_bits: compact_str::format_compact!("{:o}", mode & 0o777),
            size: node.size,
            size_physical: node.size,
            editable: false,
            inner_editable: false,
            directory: true,
            file: false,
            symlink: false,
            mime: MimeCacheValue::directory().mime,
            modified: node.mtime,
            created: chrono::DateTime::from_timestamp(0, 0).unwrap_or_default(),
        }
    }

    fn directory_entry_from_file_meta(
        path: &Path,
        meta: &KopiaFileMeta,
        buffer: Option<&[u8]>,
    ) -> DirectoryEntry {
        let detected_mime = if meta.file_type.is_symlink() {
            MimeCacheValue::symlink()
        } else if meta.file_type.is_file() && meta.size == 0 {
            MimeCacheValue::text()
        } else {
            crate::utils::detect_mime_type(path, buffer)
        };

        DirectoryEntry {
            name: path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into(),
            mode: encode_mode(meta.mode),
            mode_bits: compact_str::format_compact!("{:o}", meta.mode & 0o777),
            size: meta.size,
            size_physical: meta.size,
            editable: meta.file_type.is_file() && detected_mime.valid_utf8,
            inner_editable: meta.file_type.is_file() && detected_mime.valid_inner_utf8,
            directory: false,
            file: meta.file_type.is_file(),
            symlink: meta.file_type.is_symlink(),
            mime: detected_mime.mime,
            modified: meta.mtime,
            created: chrono::DateTime::from_timestamp(0, 0).unwrap_or_default(),
        }
    }

    fn collect_subtree(
        node: &KopiaTreeNode,
        relative_dir: &Path,
        is_ignored: &IsIgnoredFn,
        out: &mut Vec<SubtreeEntry>,
    ) {
        for (name, meta) in node.files.iter() {
            let relative = relative_dir.join(name.as_str());
            if (is_ignored)(meta.file_type, relative.clone()).is_none() {
                continue;
            }
            out.push(SubtreeEntry {
                relative,
                file_type: meta.file_type,
                mode: meta.mode,
                mtime: meta.mtime,
                size: meta.size,
                oid: meta.oid.clone(),
            });
        }

        for (name, child) in node.dirs.iter() {
            let relative = relative_dir.join(name.as_str());
            if (is_ignored)(FileType::Dir, relative.clone()).is_none() {
                continue;
            }
            let mode = if child.mode != 0 { child.mode } else { 0o755 };
            out.push(SubtreeEntry {
                relative: relative.clone(),
                file_type: FileType::Dir,
                mode,
                mtime: child.mtime,
                size: 0,
                oid: String::new(),
            });
            Self::collect_subtree(child, &relative, is_ignored, out);
        }
    }
}

#[async_trait::async_trait]
impl VirtualReadableFilesystem for VirtualKopiaBackup {
    fn backing_server(&self) -> &crate::server::Server {
        &self.server
    }

    fn metadata(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
    ) -> Result<FileMetadata, anyhow::Error> {
        let path = path.as_ref();

        if path == Path::new("") || path == Path::new("/") {
            return Ok(FileMetadata {
                file_type: FileType::Dir,
                permissions: PortablePermissions::from_mode(0o755),
                size: 0,
                modified: None,
                created: None,
            });
        }

        if let Some(node) = self.tree.lookup_dir(path) {
            let mode = if node.mode != 0 { node.mode } else { 0o755 };
            return Ok(FileMetadata {
                file_type: FileType::Dir,
                permissions: PortablePermissions::from_mode(mode),
                size: node.size,
                modified: node.has_explicit_entry.then(|| node.mtime.into()),
                created: None,
            });
        }

        if let Some(meta) = self.tree.lookup_file(path) {
            return Ok(FileMetadata {
                file_type: meta.file_type,
                permissions: PortablePermissions::from_mode(meta.mode),
                size: meta.size,
                modified: Some(meta.mtime.into()),
                created: None,
            });
        }

        Err(anyhow::anyhow!(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "File not found"
        )))
    }
    async fn async_metadata(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
    ) -> Result<FileMetadata, anyhow::Error> {
        self.metadata(path)
    }

    fn symlink_metadata(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
    ) -> Result<FileMetadata, anyhow::Error> {
        self.metadata(path)
    }
    async fn async_symlink_metadata(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
    ) -> Result<FileMetadata, anyhow::Error> {
        self.metadata(path)
    }

    async fn async_directory_entry(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
    ) -> Result<DirectoryEntry, anyhow::Error> {
        let path = path.as_ref();
        if let Some(node) = self.tree.lookup_dir(path) {
            return Ok(Self::directory_entry_from_dir_node(path, node));
        }
        if let Some(meta) = self.tree.lookup_file(path) {
            return Ok(Self::directory_entry_from_file_meta(path, meta, None));
        }
        Err(anyhow::anyhow!(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "File not found"
        )))
    }
    async fn async_directory_entry_buffer(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
        buffer: &[u8],
    ) -> Result<DirectoryEntry, anyhow::Error> {
        let path = path.as_ref();
        if let Some(node) = self.tree.lookup_dir(path) {
            return Ok(Self::directory_entry_from_dir_node(path, node));
        }
        if let Some(meta) = self.tree.lookup_file(path) {
            return Ok(Self::directory_entry_from_file_meta(
                path,
                meta,
                Some(buffer),
            ));
        }
        Err(anyhow::anyhow!(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "File not found"
        )))
    }

    async fn async_read_dir(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
        per_page: Option<usize>,
        page: usize,
        is_ignored: IsIgnoredFn,
        sort: crate::models::DirectorySortingMode,
    ) -> Result<DirectoryListing, anyhow::Error> {
        use crate::models::DirectorySortingMode::*;

        let path = path.as_ref().to_path_buf();
        let node = match self.tree.lookup_dir(&path) {
            Some(node) => node,
            None => {
                return Ok(DirectoryListing {
                    total_entries: 0,
                    entries: Vec::new(),
                });
            }
        };

        enum Child<'a> {
            Dir {
                path: PathBuf,
                node: &'a KopiaTreeNode,
            },
            File {
                path: PathBuf,
                meta: &'a KopiaFileMeta,
            },
        }

        let mut dir_children: Vec<Child<'_>> = Vec::new();
        let mut file_children: Vec<Child<'_>> = Vec::new();

        for (name, child_node) in node.dirs.iter() {
            let child_path = match (is_ignored)(FileType::Dir, path.join(name.as_str())) {
                Some(kept) => kept,
                None => continue,
            };
            dir_children.push(Child::Dir {
                path: child_path,
                node: child_node,
            });
        }
        for (name, meta) in node.files.iter() {
            let child_path = match (is_ignored)(meta.file_type, path.join(name.as_str())) {
                Some(kept) => kept,
                None => continue,
            };
            file_children.push(Child::File {
                path: child_path,
                meta,
            });
        }

        let cmp = |a: &Child<'_>, b: &Child<'_>| -> std::cmp::Ordering {
            let (a_path, a_size, a_mtime) = match a {
                Child::Dir { path, node } => (path, node.size, node.mtime),
                Child::File { path, meta } => (path, meta.size, meta.mtime),
            };
            let (b_path, b_size, b_mtime) = match b {
                Child::Dir { path, node } => (path, node.size, node.mtime),
                Child::File { path, meta } => (path, meta.size, meta.mtime),
            };

            match sort {
                NameAsc => a_path.cmp_ascii_case_insensitive(b_path),
                NameDesc => b_path.cmp_ascii_case_insensitive(a_path),
                SizeAsc | PhysicalSizeAsc => a_size.cmp(&b_size),
                SizeDesc | PhysicalSizeDesc => b_size.cmp(&a_size),
                ModifiedAsc | CreatedAsc => a_mtime.cmp(&b_mtime),
                ModifiedDesc | CreatedDesc => b_mtime.cmp(&a_mtime),
            }
        };

        dir_children.sort_unstable_by(&cmp);
        file_children.sort_unstable_by(&cmp);

        let total_entries = dir_children.len() + file_children.len();
        let merged = dir_children.into_iter().chain(file_children);

        let target: Vec<Child<'_>> = if let Some(per_page) = per_page {
            let start = page.saturating_sub(1) * per_page;
            merged.skip(start).take(per_page).collect()
        } else {
            merged.collect()
        };

        let mut entries = Vec::with_capacity(target.len());
        for child in target {
            match child {
                Child::Dir { path, node } => {
                    entries.push(Self::directory_entry_from_dir_node(&path, node));
                }
                Child::File { path, meta } => {
                    entries.push(Self::directory_entry_from_file_meta(&path, meta, None));
                }
            }
        }

        Ok(DirectoryListing {
            total_entries,
            entries,
        })
    }

    async fn async_walk_dir<'a>(
        &'a self,
        path: &(dyn AsRef<Path> + Send + Sync),
        is_ignored: IsIgnoredFn,
    ) -> Result<Box<dyn DirectoryWalk + Send + Sync + 'a>, anyhow::Error> {
        let mut flat: Vec<(FileType, PathBuf)> = Vec::new();

        if let Some(start) = self.tree.lookup_dir(path.as_ref()) {
            fn walk(
                node: &KopiaTreeNode,
                current_path: &Path,
                is_ignored: &IsIgnoredFn,
                out: &mut Vec<(FileType, PathBuf)>,
            ) {
                for (name, meta) in node.files.iter() {
                    let child_path = current_path.join(name.as_str());
                    if let Some(filtered) = (is_ignored)(meta.file_type, child_path) {
                        out.push((meta.file_type, filtered));
                    }
                }
                for (name, child) in node.dirs.iter() {
                    let child_path = current_path.join(name.as_str());
                    if let Some(filtered) = (is_ignored)(FileType::Dir, child_path.clone()) {
                        out.push((FileType::Dir, filtered));
                    }
                    walk(child, &child_path, is_ignored, out);
                }
            }

            walk(start, path.as_ref(), &is_ignored, &mut flat);
        }

        struct TreeWalk {
            items: std::vec::IntoIter<(FileType, PathBuf)>,
        }

        #[async_trait::async_trait]
        impl DirectoryWalk for TreeWalk {
            async fn next_entry(&mut self) -> Option<Result<(FileType, PathBuf), anyhow::Error>> {
                self.items.next().map(Ok)
            }
        }

        Ok(Box::new(TreeWalk {
            items: flat.into_iter(),
        }))
    }

    async fn async_walk_dir_stream<'a>(
        &'a self,
        path: &(dyn AsRef<Path> + Send + Sync),
        is_ignored: IsIgnoredFn,
    ) -> Result<Box<dyn DirectoryStreamWalk + Send + Sync + 'a>, anyhow::Error> {
        struct KopiaDirStreamWalk {
            entry_wanted_notifier: Arc<tokio::sync::Notify>,
            entry_channel_rx: tokio::sync::mpsc::Receiver<
                Result<(FileType, PathBuf, AsyncReadableFileStream), anyhow::Error>,
            >,
        }

        #[async_trait::async_trait]
        impl DirectoryStreamWalk for KopiaDirStreamWalk {
            fn supports_multithreading(&self) -> bool {
                false
            }

            async fn next_entry(
                &mut self,
            ) -> Option<Result<(FileType, PathBuf, AsyncReadableFileStream), anyhow::Error>>
            {
                self.entry_wanted_notifier.notify_one();
                self.entry_channel_rx.recv().await
            }
        }

        let mut flat: Vec<(FileType, PathBuf, String)> = Vec::new();
        if let Some(start) = self.tree.lookup_dir(path.as_ref()) {
            fn walk(
                node: &KopiaTreeNode,
                current_path: &Path,
                is_ignored: &IsIgnoredFn,
                out: &mut Vec<(FileType, PathBuf, String)>,
            ) {
                for (name, meta) in node.files.iter() {
                    let child_path = current_path.join(name.as_str());
                    if let Some(filtered) = (is_ignored)(meta.file_type, child_path) {
                        out.push((meta.file_type, filtered, meta.oid.clone()));
                    }
                }
                for (name, child) in node.dirs.iter() {
                    let child_path = current_path.join(name.as_str());
                    if let Some(filtered) = (is_ignored)(FileType::Dir, child_path.clone()) {
                        out.push((FileType::Dir, filtered, String::new()));
                    }
                    walk(child, &child_path, is_ignored, out);
                }
            }

            walk(start, path.as_ref(), &is_ignored, &mut flat);
        }

        let entry_wanted_notifier = Arc::new(tokio::sync::Notify::new());
        let (entry_channel_tx, entry_channel_rx) = tokio::sync::mpsc::channel(1);

        crate::spawn_handled({
            let entry_wanted_notifier = Arc::clone(&entry_wanted_notifier);
            let config_file = self.config_file.clone();
            let remote = Arc::clone(&self.remote);

            async move {
                for (file_type, entry_path, oid) in flat {
                    entry_wanted_notifier.notified().await;

                    if file_type.is_file() {
                        let child = KopiaBackup::get_tokio_command(&config_file, &remote)
                            .arg("show")
                            .arg(&oid)
                            .stdout(std::process::Stdio::piped())
                            .stderr(std::process::Stdio::null())
                            .spawn()?;

                        let stdout = match child.stdout {
                            Some(stdout) => stdout,
                            None => {
                                entry_channel_tx
                                    .send(Err(anyhow::anyhow!("kopia show produced no stdout")))
                                    .await?;
                                continue;
                            }
                        };

                        entry_channel_tx
                            .send(Ok((
                                file_type,
                                entry_path,
                                Box::new(stdout) as AsyncReadableFileStream,
                            )))
                            .await?;
                    } else {
                        entry_channel_tx
                            .send(Ok((
                                file_type,
                                entry_path,
                                Box::new(tokio::io::empty()) as AsyncReadableFileStream,
                            )))
                            .await?;
                    }
                }

                entry_wanted_notifier.notify_one();
                Ok::<_, anyhow::Error>(())
            }
        });

        entry_wanted_notifier.notify_one();

        Ok(Box::new(KopiaDirStreamWalk {
            entry_wanted_notifier,
            entry_channel_rx,
        }))
    }

    fn read_file(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
        _range: Option<ByteRange>,
    ) -> Result<FileRead, anyhow::Error> {
        let meta = self.metadata(path)?;
        if !meta.file_type.is_file() {
            return Err(anyhow::anyhow!(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "File not found"
            )));
        }

        let file_meta = self
            .tree
            .lookup_file(path.as_ref())
            .ok_or_else(|| anyhow::anyhow!("File not found"))?;
        let reader = self.open_object(&file_meta.oid)?;

        Ok(FileRead {
            size: meta.size,
            total_size: meta.size,
            reader_range: None,
            reader: Box::new(reader),
        })
    }
    async fn async_read_file(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
        _range: Option<ByteRange>,
    ) -> Result<AsyncFileRead, anyhow::Error> {
        let meta = self.metadata(path)?;
        if !meta.file_type.is_file() {
            return Err(anyhow::anyhow!(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "File not found"
            )));
        }

        let oid = self
            .tree
            .lookup_file(path.as_ref())
            .ok_or_else(|| anyhow::anyhow!("File not found"))?
            .oid
            .clone();

        let child = KopiaBackup::get_tokio_command(&self.config_file, &self.remote)
            .arg("show")
            .arg(&oid)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()?;

        let reader = child
            .stdout
            .ok_or_else(|| anyhow::anyhow!("kopia show produced no stdout"))?;

        Ok(AsyncFileRead {
            size: meta.size,
            total_size: meta.size,
            reader_range: None,
            reader: Box::new(reader),
        })
    }

    fn read_symlink(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
    ) -> Result<PathBuf, anyhow::Error> {
        let path = path.as_ref();
        match self.tree.lookup_file(path) {
            Some(meta) if meta.file_type.is_symlink() => {
                let mut reader = self.open_object(&meta.oid)?;
                let mut target = String::new();
                reader.read_to_string(&mut target)?;
                Ok(PathBuf::from(target))
            }
            _ => Err(anyhow::anyhow!(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Symlink not found"
            ))),
        }
    }
    async fn async_read_symlink(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
    ) -> Result<PathBuf, anyhow::Error> {
        self.read_symlink(path)
    }

    async fn async_read_dir_archive(
        &self,
        path: &(dyn AsRef<Path> + Send + Sync),
        archive_format: StreamableArchiveFormat,
        compression_level: CompressionLevel,
        bytes_archived: Option<Arc<AtomicU64>>,
        is_ignored: IsIgnoredFn,
    ) -> Result<tokio::io::ReadHalf<tokio::io::SimplexStream>, anyhow::Error> {
        let base_path = path.as_ref().to_path_buf();
        let node = match self.tree.lookup_dir(&base_path) {
            Some(node) => node,
            None => {
                return Err(anyhow::anyhow!(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "File not found"
                )));
            }
        };

        let mut entries = Vec::new();
        Self::collect_subtree(node, Path::new(""), &is_ignored, &mut entries);

        let threads = self
            .server
            .app_state
            .config
            .load()
            .api
            .file_compression_threads;
        let config_file = self.config_file.clone();
        let remote = Arc::clone(&self.remote);
        let (reader, writer) = tokio::io::simplex(crate::BUFFER_SIZE);

        let open_object = move |oid: &str| -> Result<std::process::ChildStdout, anyhow::Error> {
            let child = KopiaBackup::get_std_command(&config_file, &remote)
                .arg("show")
                .arg(oid)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .spawn()?;
            child
                .stdout
                .ok_or_else(|| anyhow::anyhow!("kopia show produced no stdout"))
        };

        match archive_format {
            StreamableArchiveFormat::Zip => {
                crate::spawn_blocking_handled(move || -> Result<(), anyhow::Error> {
                    let writer = tokio_util::io::SyncIoBridge::new(writer);
                    let mut zip = zip::ZipWriter::new_stream(writer);

                    for entry in entries {
                        let name = entry.relative.to_string_lossy();
                        let mut options: zip::write::FileOptions<'_, ()> =
                            zip::write::FileOptions::default()
                                .compression_level(
                                    Some(compression_level.to_deflate_level() as i64),
                                )
                                .unix_permissions(entry.mode)
                                .large_file(entry.size >= u32::MAX as u64);

                        if let Some(dt) =
                            chrono::DateTime::from_timestamp(entry.mtime.timestamp(), 0)
                            && let Ok(dt) = zip::DateTime::from_date_and_time(
                                dt.year() as u16,
                                dt.month() as u8,
                                dt.day() as u8,
                                dt.hour() as u8,
                                dt.minute() as u8,
                                dt.second() as u8,
                            )
                        {
                            options = options.last_modified_time(dt);
                        }

                        match entry.file_type {
                            FileType::Dir => {
                                zip.add_directory(name, options)?;
                            }
                            FileType::File => {
                                zip.start_file(name, options)?;
                                let mut reader = open_object(&entry.oid)?;
                                let mut buffer = vec![0; crate::BUFFER_SIZE];
                                loop {
                                    let read = reader.read(&mut buffer)?;
                                    if read == 0 {
                                        break;
                                    }
                                    let chunk = buffer.get(..read).unwrap_or_default();
                                    zip.write_all(chunk)?;
                                    if let Some(counter) = &bytes_archived {
                                        counter.fetch_add(read as u64, Ordering::SeqCst);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }

                    let mut inner = zip.finish()?.into_inner();
                    inner.flush()?;
                    inner.shutdown()?;
                    Ok(())
                });
            }
            f if f.is_tar() => {
                crate::spawn_blocking_handled(move || -> Result<(), anyhow::Error> {
                    let writer = CompressionWriter::new(
                        tokio_util::io::SyncIoBridge::new(writer),
                        f.compression_format(),
                        compression_level,
                        threads,
                    )?;
                    let mut tar = tar::Builder::new(writer);

                    for entry in entries {
                        let mut header = tar::Header::new_gnu();
                        header.set_mode(entry.mode);
                        header.set_mtime(entry.mtime.timestamp().max(0) as u64);
                        header.set_uid(0);
                        header.set_gid(0);

                        match entry.file_type {
                            FileType::Dir => {
                                header.set_entry_type(tar::EntryType::Directory);
                                header.set_size(0);
                                tar.append_data(&mut header, &entry.relative, std::io::empty())?;
                            }
                            FileType::File => {
                                header.set_entry_type(tar::EntryType::Regular);
                                header.set_size(entry.size);
                                let reader = open_object(&entry.oid)?;
                                let reader: Box<dyn Read> = match &bytes_archived {
                                    Some(counter) => Box::new(CountingReader::new_with_bytes_read(
                                        reader,
                                        counter.clone(),
                                    )),
                                    None => Box::new(reader),
                                };
                                let mut reader =
                                    crate::io::fixed_reader::FixedReader::new_with_fixed_bytes(
                                        reader,
                                        entry.size as usize,
                                    );
                                tar.append_data(&mut header, &entry.relative, &mut reader)?;
                            }
                            _ => {}
                        }
                    }

                    tar.finish()?;
                    let mut inner = tar.into_inner()?.finish()?;
                    inner.flush()?;
                    inner.shutdown()?;
                    Ok(())
                });
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "unsupported archive format for kopia backups: {}",
                    archive_format.extension()
                ));
            }
        }

        Ok(reader)
    }

    async fn close(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}
