use super::ServerConfigurationFile;

pub struct JsonFileParser;

#[async_trait::async_trait]
impl super::ProcessConfigurationFileParser for JsonFileParser {
    async fn process_file(
        content: &str,
        config: &ServerConfigurationFile,
        server: &crate::server::Server,
    ) -> Result<Vec<u8>, anyhow::Error> {
        tracing::debug!(
            server = %server.uuid,
            "processing json file"
        );

        let mut json = if content.trim().is_empty() {
            serde_json::Value::Object(serde_json::Map::new())
        } else {
            serde_json::from_str(content)
                .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new()))
        };

        for replacement in &config.replace {
            let value = match &replacement.replace_with {
                serde_json::Value::String(_) => {
                    let resolved = ServerConfigurationFile::replace_all_placeholders(
                        server,
                        &replacement.replace_with,
                    )
                    .await?;

                    serde_json::from_str(&resolved)
                        .unwrap_or_else(|_| serde_json::Value::String(resolved.into()))
                }
                other => other.clone(),
            };

            let path = parse_path(&replacement.r#match);
            set_nested_value(
                &mut json,
                &path,
                value,
                replacement.insert_new.unwrap_or(true),
                replacement.update_existing,
            );
        }

        Ok(serde_json::to_vec_pretty(&json)?)
    }
}

#[derive(Debug, Clone)]
pub enum PathSegment<'a> {
    Key(&'a str),
    Index(usize),
}

pub fn parse_path(raw: &str) -> Vec<PathSegment<'_>> {
    let mut out = Vec::new();

    for part in raw.split('.') {
        if part.is_empty() {
            continue;
        }

        let (key, mut rest) = match part.find('[') {
            Some(bracket) => part.split_at(bracket),
            None => {
                out.push(PathSegment::Key(part));
                continue;
            }
        };

        if !key.is_empty() {
            out.push(PathSegment::Key(key));
        }

        while !rest.is_empty() {
            let Some(end) = rest.find(']') else { break };
            let idx_str = &rest[1..end];
            if let Ok(idx) = idx_str.parse::<usize>() {
                out.push(PathSegment::Index(idx));
            }
            rest = &rest[end + 1..];
        }
    }

    out
}

pub fn set_nested_value(
    json: &mut serde_json::Value,
    path: &[PathSegment<'_>],
    value: serde_json::Value,
    insert_new: bool,
    update_existing: bool,
) {
    let Some((head, tail)) = path.split_first() else {
        return;
    };

    match head {
        PathSegment::Key(_) if !json.is_object() => {
            *json = serde_json::Value::Object(serde_json::Map::new());
        }
        PathSegment::Index(_) if !json.is_array() => {
            *json = serde_json::Value::Array(Vec::new());
        }
        _ => {}
    }

    if tail.is_empty() {
        match head {
            PathSegment::Key(k) => {
                let map = json.as_object_mut().unwrap();
                let exists = map.contains_key(*k);

                if (exists && update_existing) || (!exists && insert_new) {
                    map.insert((*k).to_string(), value);
                }
            }
            PathSegment::Index(i) => {
                let arr = json.as_array_mut().unwrap();
                let exists = *i < arr.len();

                if exists && update_existing {
                    arr[*i] = value;
                } else if !exists && insert_new {
                    while arr.len() < *i {
                        arr.push(serde_json::Value::Null);
                    }
                    arr.push(value);
                }
            }
        }
        return;
    }

    let next_is_index = matches!(tail[0], PathSegment::Index(_));
    let default_child = || {
        if next_is_index {
            serde_json::Value::Array(Vec::new())
        } else {
            serde_json::Value::Object(serde_json::Map::new())
        }
    };

    match head {
        PathSegment::Key(k) => {
            let map = json.as_object_mut().unwrap();
            let child = map.entry((*k).to_string()).or_insert_with(default_child);
            set_nested_value(child, tail, value, insert_new, update_existing);
        }
        PathSegment::Index(i) => {
            let arr = json.as_array_mut().unwrap();
            while arr.len() <= *i {
                arr.push(default_child());
            }
            set_nested_value(&mut arr[*i], tail, value, insert_new, update_existing);
        }
    }
}
