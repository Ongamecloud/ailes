use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod post {
    use crate::{
        io::counting_reader::AsyncCountingReader,
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, GetState, api::servers::_server_::GetServer},
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
        location: compact_str::CompactString,
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
        let location = match server.filesystem.async_canonicalize(data.location).await {
            Ok(path) => path,
            Err(_) => {
                return ApiResponse::error("file not found")
                    .with_status(StatusCode::NOT_FOUND)
                    .ok();
            }
        };

        let metadata = match server.filesystem.async_metadata(&location).await {
            Ok(metadata) => {
                if (!metadata.is_file() && !metadata.is_dir())
                    || server
                        .filesystem
                        .is_ignored(&location, metadata.is_dir())
                        .await
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
            server: &GetServer,
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

                if server
                    .filesystem
                    .async_symlink_metadata(&new_path)
                    .await
                    .is_err()
                {
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

        let parent = match location.parent() {
            Some(parent) => parent,
            None => {
                return ApiResponse::error("file has no parent")
                    .with_status(StatusCode::EXPECTATION_FAILED)
                    .ok();
            }
        };

        if server.filesystem.is_ignored(parent, true).await {
            return ApiResponse::error("parent directory not found")
                .with_status(StatusCode::EXPECTATION_FAILED)
                .ok();
        }

        let new_name = if let Some(name) = data.name {
            name
        } else {
            generate_new_name(&server, &location).await
        };
        let file_name = parent.join(&new_name);

        if metadata.is_file() {
            if !server
                .filesystem
                .async_allocate_in_path(parent, metadata.len() as i64, false)
                .await
            {
                return ApiResponse::error("failed to allocate space")
                    .with_status(StatusCode::EXPECTATION_FAILED)
                    .ok();
            }

            if data.foreground {
                server
                    .filesystem
                    .async_copy(&location, &server.filesystem, &file_name)
                    .await?;
            } else {
                let progress = Arc::new(AtomicU64::new(0));
                let total = Arc::new(AtomicU64::new(metadata.len()));

                let (identifier, _) = server
                    .filesystem
                    .operations
                    .add_operation(
                        crate::server::filesystem::operations::FilesystemOperation::Copy {
                            path: location.clone(),
                            destination: file_name.clone(),
                            progress: progress.clone(),
                            total,
                        },
                        {
                            let server = server.0.clone();
                            let location = location.clone();
                            let file_name = file_name.clone();

                            async move {
                                let reader = server.filesystem.async_open(&location).await?;
                                let mut writer =
                                    crate::server::filesystem::writer::AsyncFileSystemWriter::new(
                                        server.clone(),
                                        &file_name,
                                        Some(metadata.permissions()),
                                        metadata.modified().ok(),
                                    )
                                    .await?;
                                let mut counting_reader = AsyncCountingReader::new_with_bytes_read(
                                    reader,
                                    Arc::clone(&progress),
                                );

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
            let progress = Arc::new(AtomicU64::new(0));
            let total = Arc::new(AtomicU64::new(
                server
                    .filesystem
                    .disk_usage
                    .read()
                    .await
                    .get_size(&location)
                    .map_or(0, |s| s.get_apparent()),
            ));

            let (identifier, task) = server
                .filesystem
                .operations
                .add_operation(
                    crate::server::filesystem::operations::FilesystemOperation::Copy {
                        path: location.clone(),
                        destination: file_name.clone(),
                        progress: progress.clone(),
                        total,
                    },
                    {
                        let server = server.0.clone();
                        let location = location.clone();
                        let file_name = file_name.clone();

                        async move {
                            let ignored = &[server.filesystem.get_ignored().await];
                            let mut walker = server
                                .filesystem
                                .async_walk_dir(&location)
                                .await?
                                .with_ignored(ignored);

                            walker
                                .run_multithreaded(
                                    state.config.api.file_copy_threads,
                                    Arc::new({
                                        let server = server.clone();
                                        let location = Arc::new(location);
                                        let file_name = Arc::new(file_name);
                                        let progress = Arc::clone(&progress);

                                        move |_, path: PathBuf| {
                                            let server = server.clone();
                                            let location = Arc::clone(&location);
                                            let file_name = Arc::clone(&file_name);
                                            let progress = Arc::clone(&progress);

                                            async move {
                                                let metadata =
                                                    match server.filesystem.async_symlink_metadata(&path).await {
                                                        Ok(metadata) => metadata,
                                                        Err(_) => return Ok(()),
                                                    };

                                                let relative_path = match path.strip_prefix(&*location) {
                                                    Ok(p) => p,
                                                    Err(_) => return Ok(()),
                                                };
                                                let destination_path = file_name.join(relative_path);

                                                if metadata.is_file() {
                                                    if let Some(parent) = destination_path.parent() {
                                                        server.filesystem.async_create_dir_all(parent).await?;
                                                    }

                                                    let file = server.filesystem.async_open(&path).await?;
                                                    let mut writer =
                                                        crate::server::filesystem::writer::AsyncFileSystemWriter::new(
                                                            server.clone(),
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
                                                    server.filesystem.async_create_dir_all(&destination_path).await?;
                                                    server
                                                        .filesystem
                                                        .async_set_permissions(&destination_path, metadata.permissions())
                                                        .await?;
                                                    if let Ok(modified_time) = metadata.modified() {
                                                        server.filesystem.async_set_times(
                                                            &destination_path,
                                                            modified_time.into_std(),
                                                            None,
                                                        ).await?;
                                                    }

                                                    progress.fetch_add(metadata.len(), Ordering::Relaxed);
                                                } else if metadata.is_symlink() && let Ok(target) = server.filesystem.async_read_link(&path).await {
                                                    if let Err(err) = server.filesystem.async_symlink(&target, &destination_path).await {
                                                        tracing::debug!(path = %destination_path.display(), "failed to create symlink from backup: {:?}", err);
                                                    } else if let Ok(modified_time) = metadata.modified() {
                                                        server.filesystem.async_set_times(
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
                            path = %location.display(),
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
                            path = %location.display(),
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
