use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod post {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::servers::_server_::GetServer},
        server::filesystem::virtualfs::VirtualReadableFilesystem,
    };
    use axum::http::StatusCode;
    use compact_str::ToCompactString;
    use serde::{Deserialize, Serialize};
    use std::path::Path;
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

        if let Some(identifier) = server
            .filesystem
            .copy_path(
                &server,
                metadata,
                path.clone(),
                &path,
                filesystem,
                destination_path.clone(),
                &file_name,
                destination_filesystem.clone(),
                data.foreground,
            )
            .await?
        {
            ApiResponse::json(Response { identifier })
                .with_status(StatusCode::ACCEPTED)
                .ok()
        } else {
            ApiResponse::json(
                destination_filesystem
                    .async_directory_entry(&file_name)
                    .await?,
            )
            .ok()
        }
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(post::route))
        .with_state(state.clone())
}
