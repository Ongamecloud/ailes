use super::ServerConfigurationFile;
use toml_edit::{Array, ArrayOfTables, DocumentMut, InlineTable, Item, Table, TableLike, Value};

pub struct TomlFileParser;

#[async_trait::async_trait]
impl super::ProcessConfigurationFileParser for TomlFileParser {
    async fn process_file(
        content: &str,
        config: &ServerConfigurationFile,
        server: &crate::server::Server,
    ) -> Result<Vec<u8>, anyhow::Error> {
        tracing::debug!(server = %server.uuid, "processing toml file");

        let mut doc = if content.trim().is_empty() {
            DocumentMut::new()
        } else {
            content.parse::<DocumentMut>().unwrap_or_default()
        };

        for replacement in &config.replace {
            let value: Value = match &replacement.replace_with {
                serde_json::Value::String(_) => {
                    let resolved = ServerConfigurationFile::replace_all_placeholders(
                        server,
                        &replacement.replace_with,
                    )
                    .await?;

                    resolved
                        .parse::<Item>()
                        .ok()
                        .and_then(|item| item.into_value().ok())
                        .unwrap_or_else(|| Value::from(resolved.into_string()))
                }
                other => json_to_toml_value(other),
            };

            let path = super::json::parse_path(&replacement.r#match);
            set_nested_value(
                doc.as_table_mut(),
                &path,
                value,
                replacement.insert_new.unwrap_or(true),
                replacement.update_existing,
            );
        }

        Ok(doc.to_string().into_bytes())
    }
}

fn json_to_toml_value(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::from("null"),
        serde_json::Value::Bool(b) => Value::from(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::from(i)
            } else if let Some(f) = n.as_f64() {
                Value::from(f)
            } else {
                Value::from(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::from(s.clone()),
        serde_json::Value::Array(arr) => {
            let mut a = Array::new();
            for v in arr {
                a.push(json_to_toml_value(v));
            }
            Value::Array(a)
        }
        serde_json::Value::Object(map) => {
            let mut t = InlineTable::new();
            for (k, v) in map {
                t.insert(k, json_to_toml_value(v));
            }
            Value::InlineTable(t)
        }
    }
}

pub fn set_nested_value(
    table: &mut dyn TableLike,
    path: &[super::json::PathSegment<'_>],
    value: Value,
    insert_new: bool,
    update_existing: bool,
) {
    let Some((head, tail)) = path.split_first() else {
        return;
    };

    let super::json::PathSegment::Key(k) = head else {
        return;
    };

    let Some(tail_first) = tail.first() else {
        let exists = table.contains_key(k);
        if (exists && update_existing) || (!exists && insert_new) {
            table.insert(k, Item::Value(value));
        }
        return;
    };

    match tail_first {
        super::json::PathSegment::Key(_) => {
            let child = table.entry(k).or_insert(Item::Table(Table::new()));
            if let Some(child_table) = child.as_table_like_mut() {
                set_nested_value(child_table, tail, value, insert_new, update_existing);
            }
        }
        super::json::PathSegment::Index(_) => {
            set_in_array_under_key(table, k, tail, value, insert_new, update_existing);
        }
    }
}

fn set_in_array_under_key(
    table: &mut dyn TableLike,
    key: &str,
    path: &[super::json::PathSegment<'_>],
    value: Value,
    insert_new: bool,
    update_existing: bool,
) {
    let Some((super::json::PathSegment::Index(i), rest)) = path.split_first() else {
        return;
    };
    let i = *i;

    let Some(rest_first) = rest.first() else {
        let child = table
            .entry(key)
            .or_insert(Item::Value(Value::Array(Array::new())));
        let Some(arr) = child.as_array_mut() else {
            return;
        };

        if i < arr.len() {
            if update_existing {
                arr.remove(i);
                arr.insert(i, value);
            }
        } else if insert_new {
            while arr.len() < i {
                arr.push(Value::InlineTable(InlineTable::new()));
            }
            arr.push(value);
        }

        return;
    };

    if matches!(rest_first, super::json::PathSegment::Key(_)) {
        let child = table
            .entry(key)
            .or_insert(Item::ArrayOfTables(ArrayOfTables::new()));
        let Some(aot) = child.as_array_of_tables_mut() else {
            return;
        };

        if i >= aot.len() {
            if !insert_new {
                return;
            }
            while aot.len() <= i {
                aot.push(Table::new());
            }
        }
        if let Some(elem) = aot.get_mut(i) {
            set_nested_value(elem, rest, value, insert_new, update_existing);
        }
    }
}
