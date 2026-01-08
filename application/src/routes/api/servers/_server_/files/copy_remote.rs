use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod post {
    use crate::{
        io::compression::{CompressionLevel, CompressionType},
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
                        let (checksum_sender, checksum_receiver) = tokio::sync::oneshot::channel();
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
                                    compression_level: data
                                        .compression_level
                                        .unwrap_or(state.config.system.backups.compression_level),
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
                                .file_name(format!("archive.{}", data.archive_format.extension()))
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
                            let body: serde_json::Value = response.json().await.unwrap_or_default();

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

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(post::route))
        .with_state(state.clone())
}
