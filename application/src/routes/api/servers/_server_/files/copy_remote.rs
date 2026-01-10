use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod post {
    use crate::{
        io::{
            compression::{CompressionLevel, CompressionType},
            counting_reader::AsyncCountingReader,
        },
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, GetState, api::servers::_server_::GetServer},
        server::transfer::TransferArchiveFormat,
    };
    use axum::http::StatusCode;
    use futures::FutureExt;
    use serde::{Deserialize, Serialize};
    use sha1::Digest;
    use std::{
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use utoipa::ToSchema;

    fn foreground() -> bool {
        true
    }

    #[derive(ToSchema, Deserialize)]
    pub struct Payload {
        url: String,
        token: String,

        #[serde(default)]
        archive_format: TransferArchiveFormat,
        #[serde(default, deserialize_with = "crate::deserialize::deserialize_optional")]
        compression_level: Option<CompressionLevel>,

        #[serde(default)]
        root: compact_str::CompactString,
        files: Vec<compact_str::CompactString>,

        destination_server: uuid::Uuid,
        destination_path: compact_str::CompactString,

        #[serde(default = "foreground")]
        foreground: bool,
    }

    #[derive(ToSchema, Serialize)]
    pub struct Response {}

    #[derive(ToSchema, Serialize)]
    pub struct ResponseAccepted {
        identifier: uuid::Uuid,
    }

    #[utoipa::path(post, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = ACCEPTED, body = inline(ResponseAccepted)),
        (status = NOT_FOUND, body = ApiError),
        (status = EXPECTATION_FAILED, body = ApiError),
    ), params(
        (
            "server" = uuid::Uuid,
            description = "The server uuid",
            example = "123e4567-e89b-12d3-a456-426614174000",
        ),
    ), request_body = inline(Payload))]
    pub async fn route(
        state: GetState,
        server: GetServer,
        axum::Json(data): axum::Json<Payload>,
    ) -> ApiResponseResult {
        let root = match server.filesystem.async_canonicalize(&data.root).await {
            Ok(path) => path,
            Err(_) => {
                return ApiResponse::error("file not found")
                    .with_status(StatusCode::NOT_FOUND)
                    .ok();
            }
        };

        let metadata = server.filesystem.async_symlink_metadata(&root).await;
        if !metadata.map(|m| m.is_dir()).unwrap_or(true) {
            return ApiResponse::error("root is not a directory")
                .with_status(StatusCode::EXPECTATION_FAILED)
                .ok();
        }

        let mut total_size = 0;
        for file in &data.files {
            if let Ok(metadata) = server.filesystem.async_metadata(file).await {
                if metadata.is_dir() {
                    total_size += server
                        .filesystem
                        .disk_usage
                        .read()
                        .await
                        .get_size(&root.join(file))
                        .map_or(0, |s| s.get_apparent());
                } else {
                    total_size += metadata.len();
                }
            }
        }

        let progress = Arc::new(AtomicU64::new(0));
        let total = Arc::new(AtomicU64::new(total_size));

        if data.url.is_empty() {
            let destination_server = match state
                .server_manager
                .get_servers()
                .await
                .iter()
                .find(|s| s.uuid == data.destination_server)
                .cloned()
            {
                Some(server) => server,
                None => {
                    return ApiResponse::error("destination server not found")
                        .with_status(StatusCode::NOT_FOUND)
                        .ok();
                }
            };

            let (tx, rx) = tokio::sync::oneshot::channel::<()>();

            let (identifier, task) = server
                .filesystem
                .operations
                .add_operation(
                    crate::server::filesystem::operations::FilesystemOperation::CopyRemote {
                        server: server.uuid,
                        path: root.clone(),
                        destination_server: data.destination_server,
                        destination_path: PathBuf::from(data.destination_path.clone()),
                        progress: progress.clone(),
                        total: total.clone(),
                    },
                    {
                        let root = root.clone();
                        let destination_path = PathBuf::from(data.destination_path.clone());
                        let server = server.clone();
                        let destination_server = destination_server.clone();
                        let progress = progress.clone();

                        async move {
                            let inner = async {
                                let ignored = &[
                                    server.filesystem.get_ignored().await,
                                    destination_server.filesystem.get_ignored().await,
                                ];

                                for file in &data.files {
                                    let Ok(metadata) = server.filesystem.async_metadata(file).await
                                    else {
                                        continue;
                                    };

                                    let source_path = root.join(file);
                                    let destination_path = destination_path.join(file);

                                    if metadata.is_dir() {
                                        let mut walker = server
                                            .filesystem
                                            .async_walk_dir(&source_path)
                                            .await?
                                            .with_ignored(ignored);

                                        walker
                                            .run_multithreaded(
                                                state.config.api.file_copy_threads,
                                                Arc::new({
                                                    let server = server.clone();
                                                    let destination_server = destination_server.clone();
                                                    let source_path = Arc::new(source_path);
                                                    let destination_path = Arc::new(destination_path);
                                                    let progress = Arc::clone(&progress);

                                                    move |_, path: PathBuf| {
                                                        let server = server.clone();
                                                        let destination_server = destination_server.clone();
                                                        let source_path = Arc::clone(&source_path);
                                                        let destination_path = Arc::clone(&destination_path);
                                                        let progress = Arc::clone(&progress);

                                                        async move {
                                                            let metadata =
                                                                match server.filesystem.async_symlink_metadata(&path).await {
                                                                    Ok(metadata) => metadata,
                                                                    Err(_) => return Ok(()),
                                                                };

                                                            let relative_path = match path.strip_prefix(&*source_path) {
                                                                Ok(p) => p,
                                                                Err(_) => return Ok(()),
                                                            };
                                                            let source_path = source_path.join(relative_path);
                                                            let destination_path = destination_path.join(relative_path);

                                                            if metadata.is_file() {
                                                                if let Some(parent) = destination_path.parent() {
                                                                    destination_server.filesystem.async_create_dir_all(parent).await?;
                                                                }

                                                                let file = server.filesystem.async_open(&source_path).await?;
                                                                let mut writer =
                                                                    crate::server::filesystem::writer::AsyncFileSystemWriter::new(
                                                                        destination_server.clone(),
                                                                        &destination_path,
                                                                        Some(metadata.permissions()),
                                                                        metadata.modified().ok(),
                                                                    )
                                                                    .await?;
                                                                let mut reader = AsyncCountingReader::new_with_bytes_read(
                                                                    file,
                                                                    Arc::clone(&progress),
                                                                );

                                                                tokio::io::copy(&mut reader, &mut writer).await?;
                                                                writer.shutdown().await?;
                                                            } else if metadata.is_dir() {
                                                                destination_server.filesystem.async_create_dir_all(&destination_path).await?;
                                                                destination_server
                                                                    .filesystem
                                                                    .async_set_permissions(&destination_path, metadata.permissions())
                                                                    .await?;
                                                                if let Ok(modified_time) = metadata.modified() {
                                                                    destination_server.filesystem.async_set_times(
                                                                        &destination_path,
                                                                        modified_time.into_std(),
                                                                        None,
                                                                    ).await?;
                                                                }

                                                                progress.fetch_add(metadata.len(), Ordering::Relaxed);
                                                            } else if metadata.is_symlink() && let Ok(target) = server.filesystem.async_read_link(&source_path).await {
                                                                if let Err(err) = destination_server.filesystem.async_symlink(&target, &destination_path).await {
                                                                    tracing::debug!(path = %destination_path.display(), "failed to create symlink from copy: {:?}", err);
                                                                } else if let Ok(modified_time) = metadata.modified() {
                                                                    destination_server.filesystem.async_set_times(
                                                                        &destination_path,
                                                                        modified_time.into_std(),
                                                                        None,
                                                                    ).await?;
                                                                }
                                                            }

                                                            Ok(())
                                                        }
                                                    }
                                                }),
                                            )
                                            .await?;
                                    } else if metadata.is_file() {
                                        let file = server.filesystem.async_open(source_path).await?;
                                        let mut writer =
                                            crate::server::filesystem::writer::AsyncFileSystemWriter::new(
                                                destination_server.clone(),
                                                &destination_path,
                                                Some(metadata.permissions()),
                                                metadata.modified().ok(),
                                            )
                                            .await?;
                                        let mut reader =
                                            AsyncCountingReader::new_with_bytes_read(
                                                file,
                                                Arc::clone(&progress),
                                            );

                                        tokio::io::copy(&mut reader, &mut writer).await?;
                                        writer.shutdown().await?;
                                    } else if metadata.is_symlink() && let Ok(target) = server.filesystem.async_read_link(source_path).await {
                                        if let Err(err) = destination_server.filesystem.async_symlink(&target, &destination_path).await {
                                            tracing::debug!(path = %destination_path.display(), "failed to create symlink from copy: {:?}", err);
                                        } else if let Ok(modified_time) = metadata.modified() {
                                            destination_server.filesystem.async_set_times(
                                                &destination_path,
                                                modified_time.into_std(),
                                                None,
                                            ).await?;
                                        }
                                    }
                                }

                                Ok(())
                            };

                            tokio::select! {
                                res = inner => res,
                                _ = rx =>
                                    Err(anyhow::anyhow!("copy process aborted by another source"))
                            }
                        }
                    },
                )
                .await;

            let (_, destination_task) = destination_server
                .filesystem
                .operations
                .add_operation(
                    crate::server::filesystem::operations::FilesystemOperation::CopyRemote {
                        server: server.uuid,
                        path: root.clone(),
                        destination_server: data.destination_server,
                        destination_path: PathBuf::from(data.destination_path),
                        progress: progress.clone(),
                        total: total.clone(),
                    },
                    async move {
                        let _tx = tx;

                        match task.await {
                            Ok(Some(Ok(()))) => Ok(()),
                            Ok(None) => {
                                Err(anyhow::anyhow!("copy process aborted by another source"))
                            }
                            Ok(Some(Err(err))) => Err(err),
                            Err(err) => Err(err.into()),
                        }
                    },
                )
                .await;

            if data.foreground {
                match destination_task.await {
                    Ok(Some(Ok(()))) => {}
                    Ok(None) => {
                        return ApiResponse::error("copy process aborted by another source")
                            .with_status(StatusCode::EXPECTATION_FAILED)
                            .ok();
                    }
                    Ok(Some(Err(err))) => {
                        tracing::error!(
                            server = %server.uuid,
                            root = %root.display(),
                            "failed to copy to a remote: {:#?}",
                            err,
                        );

                        return ApiResponse::error(&format!("failed to copy to a remote: {err}"))
                            .with_status(StatusCode::EXPECTATION_FAILED)
                            .ok();
                    }
                    Err(err) => {
                        tracing::error!(
                            server = %server.uuid,
                            root = %root.display(),
                            "failed to copy to a remote: {:#?}",
                            err,
                        );

                        return ApiResponse::error("failed to copy to a remote")
                            .with_status(StatusCode::EXPECTATION_FAILED)
                            .ok();
                    }
                }

                ApiResponse::json(Response {}).ok()
            } else {
                ApiResponse::json(ResponseAccepted { identifier })
                    .with_status(StatusCode::ACCEPTED)
                    .ok()
            }
        } else {
            let (identifier, task) = server
                .filesystem
                .operations
                .add_operation(
                    crate::server::filesystem::operations::FilesystemOperation::CopyRemote {
                        server: server.uuid,
                        path: root.clone(),
                        destination_server: data.destination_server,
                        destination_path: PathBuf::from(data.destination_path),
                        progress: progress.clone(),
                        total: total.clone(),
                    },
                    {
                        let root = root.clone();
                        let server = server.clone();

                        async move {
                            let (checksum_sender, checksum_receiver) =
                                tokio::sync::oneshot::channel();
                            let (checksummed_writer, mut checksummed_reader) =
                                tokio::io::duplex(crate::BUFFER_SIZE);
                            let (mut writer, reader) = tokio::io::duplex(crate::BUFFER_SIZE);

                            let archive_task = async {
                                let ignored = server.filesystem.get_ignored().await;
                                let writer = tokio_util::io::SyncIoBridge::new(checksummed_writer);

                                crate::server::filesystem::archive::create::create_tar(
                                    server.filesystem.clone(),
                                    writer,
                                    &root,
                                    data.files.into_iter().map(PathBuf::from).collect(),
                                    Some(progress),
                                    vec![ignored],
                                    crate::server::filesystem::archive::create::CreateTarOptions {
                                        compression_type: match data.archive_format {
                                            TransferArchiveFormat::Tar => CompressionType::None,
                                            TransferArchiveFormat::TarGz => CompressionType::Gz,
                                            TransferArchiveFormat::TarXz => CompressionType::Xz,
                                            TransferArchiveFormat::TarBz2 => CompressionType::Bz2,
                                            TransferArchiveFormat::TarLz4 => CompressionType::Lz4,
                                            TransferArchiveFormat::TarZstd => CompressionType::Zstd,
                                        },
                                        compression_level: data.compression_level.unwrap_or(
                                            state.config.system.backups.compression_level,
                                        ),
                                        threads: state.config.api.file_compression_threads,
                                    },
                                )
                                .await?;

                                Ok::<_, anyhow::Error>(())
                            };

                            let checksum_task = async {
                                let mut hasher = sha2::Sha256::new();

                                let mut buffer = vec![0; crate::BUFFER_SIZE];
                                loop {
                                    let bytes_read = checksummed_reader.read(&mut buffer).await?;
                                    if crate::unlikely(bytes_read == 0) {
                                        break;
                                    }

                                    hasher.update(&buffer[..bytes_read]);
                                    writer.write_all(&buffer[..bytes_read]).await?;
                                }

                                checksum_sender
                                    .send(format!("{:x}", hasher.finalize()))
                                    .ok();
                                writer.shutdown().await?;

                                Ok::<_, anyhow::Error>(())
                            };

                            let form = reqwest::multipart::Form::new()
                                .part(
                                    "archive",
                                    reqwest::multipart::Part::stream(reqwest::Body::wrap_stream(
                                        tokio_util::io::ReaderStream::with_capacity(
                                            reader,
                                            crate::BUFFER_SIZE,
                                        ),
                                    ))
                                    .file_name(format!(
                                        "archive.{}",
                                        data.archive_format.extension()
                                    ))
                                    .mime_str("application/x-tar")
                                    .unwrap(),
                                )
                                .part(
                                    "checksum",
                                    reqwest::multipart::Part::stream(reqwest::Body::wrap_stream(
                                        checksum_receiver.into_stream(),
                                    ))
                                    .file_name("checksum")
                                    .mime_str("text/plain")
                                    .unwrap(),
                                )
                                .part("test", reqwest::multipart::Part::text("JOHN PORK"));

                            let response = reqwest::Client::new()
                                .post(&data.url)
                                .header("Authorization", &data.token)
                                .header("Total-Bytes", total.load(Ordering::Relaxed))
                                .multipart(form)
                                .send();

                            let (_, _, response) =
                                tokio::try_join!(archive_task, checksum_task, async {
                                    Ok(response.await?)
                                })?;

                            if !response.status().is_success() {
                                let status = response.status();
                                let body: serde_json::Value =
                                    response.json().await.unwrap_or_default();

                                if let Some(message) = body.get("error").and_then(|m| m.as_str()) {
                                    return Err(anyhow::anyhow!(message.to_string()));
                                } else {
                                    return Err(anyhow::anyhow!(
                                        "remote server responded with an error (status: {status})"
                                    ));
                                }
                            }

                            Ok(())
                        }
                    },
                )
                .await;

            if data.foreground {
                match task.await {
                    Ok(Some(Ok(()))) => {}
                    Ok(None) => {
                        return ApiResponse::error("copy process aborted by another source")
                            .with_status(StatusCode::EXPECTATION_FAILED)
                            .ok();
                    }
                    Ok(Some(Err(err))) => {
                        tracing::error!(
                            server = %server.uuid,
                            root = %root.display(),
                            "failed to copy to a remote: {:#?}",
                            err,
                        );

                        return ApiResponse::error(&format!("failed to copy to a remote: {err}"))
                            .with_status(StatusCode::EXPECTATION_FAILED)
                            .ok();
                    }
                    Err(err) => {
                        tracing::error!(
                            server = %server.uuid,
                            root = %root.display(),
                            "failed to copy to a remote: {:#?}",
                            err,
                        );

                        return ApiResponse::error("failed to copy to a remote")
                            .with_status(StatusCode::EXPECTATION_FAILED)
                            .ok();
                    }
                }

                ApiResponse::json(Response {}).ok()
            } else {
                ApiResponse::json(ResponseAccepted { identifier })
                    .with_status(StatusCode::ACCEPTED)
                    .ok()
            }
        }
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(post::route))
        .with_state(state.clone())
}
