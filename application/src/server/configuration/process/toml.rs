use super::ServerConfigurationFile;
use serde::Deserialize;

pub struct TomlFileParser;

#[async_trait::async_trait]
impl super::ProcessConfigurationFileParser for TomlFileParser {
    async fn process_file(
        content: &str,
        config: &ServerConfigurationFile,
        server: &crate::server::Server,
    ) -> Result<Vec<u8>, anyhow::Error> {
        tracing::debug!(
            server = %server.uuid,
            "processing toml file"
        );

        let mut toml = if content.trim().is_empty() {
            toml::Value::Table(toml::map::Map::new())
        } else {
            toml::from_str(content).unwrap_or_else(|_| toml::Value::Table(toml::map::Map::new()))
        };

        for replacement in &config.replace {
            let value = match &replacement.replace_with {
                serde_json::Value::String(_) => {
                    let resolved = ServerConfigurationFile::replace_all_placeholders(
                        server,
                        &replacement.replace_with,
                    )
                    .await?;

                    toml::de::ValueDeserializer::parse(&resolved).map_or_else(
                        |_| toml::Value::String(resolved.to_string()),
                        |v| {
                            toml::Value::deserialize(v)
                                .unwrap_or_else(|_| toml::Value::String(resolved.to_string()))
                        },
                    )
                }
                other => toml::Value::try_from(other.clone())
                    .unwrap_or_else(|_| toml::Value::String(other.to_string())),
            };

            let path = super::json::parse_path(&replacement.r#match);
            set_nested_value(
                &mut toml,
                &path,
                value,
                replacement.insert_new.unwrap_or(true),
                replacement.update_existing,
            );
        }

        Ok(toml::to_string_pretty(&toml)?.into_bytes())
    }
}

pub fn set_nested_value(
    toml: &mut toml::Value,
    path: &[super::json::PathSegment<'_>],
    value: toml::Value,
    insert_new: bool,
    update_existing: bool,
) {
    let Some((head, tail)) = path.split_first() else {
        return;
    };

    match head {
        super::json::PathSegment::Key(_) if !toml.is_table() => {
            *toml = toml::Value::Table(toml::map::Map::new());
        }
        super::json::PathSegment::Index(_) if !toml.is_array() => {
            *toml = toml::Value::Array(Vec::new());
        }
        _ => {}
    }

    if tail.is_empty() {
        match head {
            super::json::PathSegment::Key(k) => {
                let map = toml.as_table_mut().unwrap();
                let exists = map.contains_key(*k);

                if (exists && update_existing) || (!exists && insert_new) {
                    map.insert((*k).to_string(), value);
                }
            }
            super::json::PathSegment::Index(i) => {
                let arr = toml.as_array_mut().unwrap();
                let exists = *i < arr.len();

                if exists && update_existing {
                    arr[*i] = value;
                } else if !exists && insert_new {
                    while arr.len() < *i {
                        arr.push(toml::Value::Table(toml::map::Map::new()));
                    }
                    arr.push(value);
                }
            }
        }
        return;
    }

    let next_is_index = matches!(tail[0], super::json::PathSegment::Index(_));
    let default_child = || {
        if next_is_index {
            toml::Value::Array(Vec::new())
        } else {
            toml::Value::Table(toml::map::Map::new())
        }
    };

    match head {
        super::json::PathSegment::Key(k) => {
            let map = toml.as_table_mut().unwrap();
            let child = map.entry((*k).to_string()).or_insert_with(default_child);
            set_nested_value(child, tail, value, insert_new, update_existing);
        }
        super::json::PathSegment::Index(i) => {
            let arr = toml.as_array_mut().unwrap();
            while arr.len() <= *i {
                arr.push(default_child());
            }
            set_nested_value(&mut arr[*i], tail, value, insert_new, update_existing);
        }
    }
}
