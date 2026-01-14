use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod post {
    use crate::{
        io::counting_reader::AsyncCountingReader,
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, GetState, api::servers::_server_::GetServer},
        server::filesystem::virtualfs::{DirectoryStreamWalkFn, VirtualReadableFilesystem},
    };
    use axum::http::StatusCode;
    use compact_str::ToCompactString;
    use serde::{Deserialize, Serialize};
    use std::{
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };
    use tokio::io::AsyncWriteExt;
    use utoipa::ToSchema;

    fn foreground() -> bool {
        true
    }

    #[derive(ToSchema, Deserialize)]
    pub struct Payload {
        #[serde(alias = "location")]
        path: compact_str::CompactString,
        name: Option<compact_str::CompactString>,

        #[serde(default = "foreground")]
        foreground: bool,
    }

    #[derive(ToSchema, Serialize)]
    pub struct Response {
        identifier: uuid::Uuid,
    }

    #[utoipa::path(post, path = "/", responses(
        (status = OK, body = crate::models::DirectoryEntry),
        (status = ACCEPTED, body = inline(Response)),
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
        let parent = match Path::new(&data.path).parent() {
            Some(parent) => parent,
            None => {
                return ApiResponse::error("file has no parent")
                    .with_status(StatusCode::EXPECTATION_FAILED)
                    .ok();
            }
        };

        let file_name = match Path::new(&data.path).file_name() {
            Some(name) => name,
            None => {
                return ApiResponse::error("invalid file name")
                    .with_status(StatusCode::EXPECTATION_FAILED)
                    .ok();
            }
        };

        let (root, filesystem) = server.filesystem.resolve_readable_fs(&server, parent).await;
        let path = root.join(file_name);

        let metadata = match filesystem.async_metadata(&path).await {
            Ok(metadata) => {
                if (!metadata.file_type.is_file() && !metadata.file_type.is_dir())
                    || (filesystem.is_primary_server_fs()
                        && server
                            .filesystem
                            .is_ignored(&path, metadata.file_type.is_dir())
                            .await)
                {
                    return ApiResponse::error("file not found")
                        .with_status(StatusCode::NOT_FOUND)
                        .ok();
                } else {
                    metadata
                }
            }
            Err(_) => {
                return ApiResponse::error("file not found")
                    .with_status(StatusCode::NOT_FOUND)
                    .ok();
            }
        };

        #[inline]
        async fn generate_new_name(
            filesystem: &dyn VirtualReadableFilesystem,
            location: &Path,
        ) -> compact_str::CompactString {
            let mut extension = location
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| compact_str::format_compact!(".{ext}"))
                .unwrap_or("".into());
            let mut base_name = location
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or("")
                .to_compact_string();

            if base_name.ends_with(".tar") {
                extension = compact_str::format_compact!(".tar{extension}");
                base_name.truncate(base_name.len() - 4);
            }

            let parent = location.parent().unwrap_or(Path::new(""));
            let mut suffix = " copy".to_compact_string();

            for i in 0..51 {
                if i > 0 {
                    suffix = compact_str::format_compact!(" copy {i}");
                }

                let new_name = compact_str::format_compact!("{base_name}{suffix}{extension}");
                let new_path = parent.join(&new_name);

                if filesystem.async_symlink_metadata(&new_path).await.is_err() {
                    return new_name;
                }

                if i == 50 {
                    let timestamp = chrono::Utc::now().to_rfc3339();
                    suffix = compact_str::format_compact!("copy.{timestamp}");

                    let final_name = compact_str::format_compact!("{base_name}{suffix}{extension}");
                    return final_name;
                }
            }

            compact_str::format_compact!("{base_name}{suffix}{extension}")
        }

        let parent = match Path::new(&data.path).parent() {
            Some(parent) => parent,
            None => {
                return ApiResponse::error("file has no parent")
                    .with_status(StatusCode::EXPECTATION_FAILED)
                    .ok();
            }
        };

        if filesystem.is_primary_server_fs() && server.filesystem.is_ignored(parent, true).await {
            return ApiResponse::error("parent directory not found")
                .with_status(StatusCode::EXPECTATION_FAILED)
                .ok();
        }

        let new_name = if let Some(name) = data.name {
            name
        } else {
            generate_new_name(&*filesystem, &path).await
        };
        let file_name = parent.join(&new_name);

        let (destination_path, destination_filesystem) = server
            .filesystem
            .resolve_writable_fs(&server, &file_name)
            .await;

        if metadata.file_type.is_file() {
            if data.foreground {
                if filesystem.is_primary_server_fs()
                    && destination_filesystem.is_primary_server_fs()
                {
                    if destination_filesystem.is_primary_server_fs()
                        && !server
                            .filesystem
                            .async_allocate_in_path(parent, metadata.size as i64, false)
                            .await
                    {
                        return ApiResponse::error("failed to allocate space")
                            .with_status(StatusCode::EXPECTATION_FAILED)
                            .ok();
                    }

                    server
                        .filesystem
                        .async_copy(&path, &server.filesystem, &destination_path)
                        .await?;
                } else {
                    let progress = Arc::new(AtomicU64::new(0));
                    let total = Arc::new(AtomicU64::new(metadata.size));

                    let (_, task) = server
                        .filesystem
                        .operations
                        .add_operation(
                            crate::server::filesystem::operations::FilesystemOperation::Copy {
                                path: PathBuf::from(data.path),
                                destination_path: file_name.clone(),
                                progress: progress.clone(),
                                total,
                            },
                            {
                                let path = path.clone();

                                async move {
                                    let file_read = filesystem.async_read_file(&path, None).await?;
                                    let mut reader = AsyncCountingReader::new_with_bytes_read(
                                        file_read.reader,
                                        Arc::clone(&progress),
                                    );

                                    let mut writer = destination_filesystem
                                        .async_create_file(&destination_path)
                                        .await?;
                                    destination_filesystem
                                        .async_set_permissions(
                                            &destination_path,
                                            metadata.permissions,
                                        )
                                        .await?;

                                    tokio::io::copy(&mut reader, &mut writer).await?;
                                    writer.shutdown().await?;

                                    Ok(())
                                }
                            },
                        )
                        .await;

                    match task.await {
                        Ok(Some(Ok(()))) => {}
                        Ok(None) => {
                            return ApiResponse::error(
                                "archive compression aborted by another source",
                            )
                            .with_status(StatusCode::EXPECTATION_FAILED)
                            .ok();
                        }
                        Ok(Some(Err(err))) => {
                            tracing::error!(
                                server = %server.uuid,
                                path = %path.display(),
                                "failed to copy directory: {:#?}",
                                err,
                            );

                            return ApiResponse::error(&format!("failed to copy directory: {err}"))
                                .with_status(StatusCode::EXPECTATION_FAILED)
                                .ok();
                        }
                        Err(err) => {
                            tracing::error!(
                                server = %server.uuid,
                                path = %path.display(),
                                "failed to copy directory: {:#?}",
                                err,
                            );

                            return ApiResponse::error("failed to copy directory")
                                .with_status(StatusCode::EXPECTATION_FAILED)
                                .ok();
                        }
                    }
                }
            } else {
                let progress = Arc::new(AtomicU64::new(0));
                let total = Arc::new(AtomicU64::new(metadata.size));

                let (identifier, _) = server
                    .filesystem
                    .operations
                    .add_operation(
                        crate::server::filesystem::operations::FilesystemOperation::Copy {
                            path: PathBuf::from(data.path),
                            destination_path: file_name.clone(),
                            progress: progress.clone(),
                            total,
                        },
                        {
                            let path = path.clone();

                            async move {
                                let file_read = filesystem.async_read_file(&path, None).await?;
                                let mut counting_reader = AsyncCountingReader::new_with_bytes_read(
                                    file_read.reader,
                                    Arc::clone(&progress),
                                );

                                let mut writer = destination_filesystem
                                    .async_create_file(&destination_path)
                                    .await?;
                                destination_filesystem
                                    .async_set_permissions(&destination_path, metadata.permissions)
                                    .await?;

                                tokio::io::copy(&mut counting_reader, &mut writer).await?;
                                writer.shutdown().await?;

                                Ok(())
                            }
                        },
                    )
                    .await;

                return ApiResponse::json(Response { identifier })
                    .with_status(StatusCode::ACCEPTED)
                    .ok();
            }
        } else {
            let directory_entry = filesystem.async_directory_entry_buffer(&path, &[]).await?;
            let progress = Arc::new(AtomicU64::new(0));
            let total = Arc::new(AtomicU64::new(directory_entry.size));

            let (identifier, task) = server
                .filesystem
                .operations
                .add_operation(
                    crate::server::filesystem::operations::FilesystemOperation::Copy {
                        path: PathBuf::from(data.path),
                        destination_path: file_name.clone(),
                        progress: progress.clone(),
                        total,
                    },
                    {
                        let server = server.0.clone();
                        let path = path.clone();

                        async move {
                            let ignored = server.filesystem.get_ignored().await;
                            let mut walker = filesystem
                                .async_walk_dir_stream(&path, ignored.into())
                                .await?;

                            walker
                                .run_multithreaded(
                                    state.config.api.file_copy_threads,
                                    DirectoryStreamWalkFn::from({
                                        let filesystem = filesystem.clone();
                                        let source_path = Arc::new(path);
                                        let destination_path = Arc::new(destination_path);
                                        let destination_filesystem = destination_filesystem.clone();
                                        let progress = Arc::clone(&progress);

                                        move |_, path: PathBuf, stream| {
                                            let filesystem = filesystem.clone();
                                            let source_path = Arc::clone(&source_path);
                                            let destination_path = Arc::clone(&destination_path);
                                            let destination_filesystem = destination_filesystem.clone();
                                            let progress = Arc::clone(&progress);

                                            async move {
                                                let metadata =
                                                    match filesystem.async_symlink_metadata(&path).await {
                                                        Ok(metadata) => metadata,
                                                        Err(_) => return Ok(()),
                                                    };

                                                let relative_path = match path.strip_prefix(&*source_path) {
                                                    Ok(p) => p,
                                                    Err(_) => return Ok(()),
                                                };
                                                let destination_path = destination_path.join(relative_path);

                                                if metadata.file_type.is_file() {
                                                    if let Some(parent) = destination_path.parent() {
                                                        destination_filesystem.async_create_dir_all(&parent).await?;
                                                    }

                                                    let mut reader = AsyncCountingReader::new_with_bytes_read(
                                                        stream,
                                                        Arc::clone(&progress),
                                                    );

                                                    let mut writer = destination_filesystem
                                                        .async_create_file(&destination_path)
                                                        .await?;
                                                    destination_filesystem
                                                        .async_set_permissions(&destination_path, metadata.permissions)
                                                        .await?;

                                                    tokio::io::copy(&mut reader, &mut writer).await?;
                                                    writer.shutdown().await?;
                                                } else if metadata.file_type.is_dir() {
                                                    destination_filesystem.async_create_dir_all(&destination_path).await?;
                                                    destination_filesystem
                                                        .async_set_permissions(&destination_path, metadata.permissions)
                                                        .await?;

                                                    progress.fetch_add(metadata.size, Ordering::Relaxed);
                                                } else if metadata.file_type.is_symlink() && let Ok(target) = filesystem.async_read_symlink(&path).await
                                                    && let Err(err) = destination_filesystem.async_create_symlink(&target, &destination_path).await {
                                                        tracing::debug!(path = %destination_path.display(), "failed to create symlink from copy: {:?}", err);
                                                    }

                                                Ok(())
                                            }
                                        }
                                    }),
                                )
                                .await?;

                            Ok(())
                        }
                    },
                )
                .await;

            if data.foreground {
                match task.await {
                    Ok(Some(Ok(()))) => {}
                    Ok(None) => {
                        return ApiResponse::error("archive compression aborted by another source")
                            .with_status(StatusCode::EXPECTATION_FAILED)
                            .ok();
                    }
                    Ok(Some(Err(err))) => {
                        tracing::error!(
                            server = %server.uuid,
                            path = %path.display(),
                            "failed to copy directory: {:#?}",
                            err,
                        );

                        return ApiResponse::error(&format!("failed to copy directory: {err}"))
                            .with_status(StatusCode::EXPECTATION_FAILED)
                            .ok();
                    }
                    Err(err) => {
                        tracing::error!(
                            server = %server.uuid,
                            path = %path.display(),
                            "failed to copy directory: {:#?}",
                            err,
                        );

                        return ApiResponse::error("failed to copy directory")
                            .with_status(StatusCode::EXPECTATION_FAILED)
                            .ok();
                    }
                }
            } else {
                return ApiResponse::json(Response { identifier })
                    .with_status(StatusCode::ACCEPTED)
                    .ok();
            }
        }

        let metadata = server.filesystem.async_metadata(&file_name).await?;

        ApiResponse::json(server.filesystem.to_api_entry(file_name, metadata).await).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(post::route))
        .with_state(state.clone())
}
