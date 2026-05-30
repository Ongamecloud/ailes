use super::ServerConfigurationFile;
use compact_str::ToCompactString;

pub struct IniFileParser;

struct PendingReplacement {
    section: Option<compact_str::CompactString>,
    key: compact_str::CompactString,
    value: compact_str::CompactString,
    insert_new: bool,
    update_existing: bool,
    applied: bool,
}

#[async_trait::async_trait]
impl super::ProcessConfigurationFileParser for IniFileParser {
    async fn process_file(
        content: &str,
        config: &ServerConfigurationFile,
        server: &crate::server::Server,
    ) -> Result<Vec<u8>, anyhow::Error> {
        tracing::debug!(
            server = %server.uuid,
            "processing ini file"
        );

        let mut pending: Vec<PendingReplacement> = Vec::with_capacity(config.replace.len());
        for replacement in &config.replace {
            let value = ServerConfigurationFile::replace_all_placeholders(
                server,
                &replacement.replace_with,
            )
            .await?;

            let (section, key) = parse_ini_path(&replacement.r#match);
            pending.push(PendingReplacement {
                section: if section.is_empty() {
                    None
                } else {
                    Some(section)
                },
                key,
                value,
                insert_new: replacement.insert_new.unwrap_or(true),
                update_existing: replacement.update_existing,
                applied: false,
            });
        }

        let newline = if content.contains("\r\n") {
            "\r\n"
        } else {
            "\n"
        };
        let mut out = String::with_capacity(content.len() + 64);
        let mut current_section = None;

        for item in ini_roundtrip::Parser::new(content) {
            match item {
                ini_roundtrip::Item::SectionEnd => {
                    for p in pending.iter_mut() {
                        if !p.applied && p.insert_new && p.section == current_section {
                            out.push_str(&p.key);
                            out.push('=');
                            out.push_str(&p.value);
                            out.push_str(newline);
                            p.applied = true;
                        }
                    }
                }
                ini_roundtrip::Item::Section { name, raw } => {
                    current_section = Some(name.to_compact_string());
                    out.push_str(raw);
                    out.push_str(newline);
                }
                ini_roundtrip::Item::Property { key, val: _, raw } => {
                    let matched = pending
                        .iter_mut()
                        .find(|p| !p.applied && p.section == current_section && p.key == key);

                    match matched {
                        Some(p) => {
                            p.applied = true;
                            if p.update_existing {
                                out.push_str(&rewrite_property(raw, key, &p.value));
                            } else {
                                out.push_str(raw);
                            }
                            out.push_str(newline);
                        }
                        None => {
                            out.push_str(raw);
                            out.push_str(newline);
                        }
                    }
                }
                ini_roundtrip::Item::Comment { raw }
                | ini_roundtrip::Item::Blank { raw }
                | ini_roundtrip::Item::Error(raw) => {
                    out.push_str(raw);
                    out.push_str(newline);
                }
            }
        }

        let mut seen_sections: Vec<&str> = Vec::new();
        for p in &pending {
            if let (false, true, Some(section)) = (p.applied, p.insert_new, p.section.as_deref())
                && !seen_sections.contains(&section)
            {
                seen_sections.push(section);
            }
        }

        for section in seen_sections {
            if !out.is_empty() {
                out.push_str(newline);
            }
            out.push('[');
            out.push_str(section);
            out.push(']');
            out.push_str(newline);

            for p in &pending {
                if !p.applied && p.insert_new && p.section.as_deref() == Some(section) {
                    out.push_str(&p.key);
                    out.push('=');
                    out.push_str(&p.value);
                    out.push_str(newline);
                }
            }
        }

        Ok(out.into_bytes())
    }
}

fn rewrite_property(raw: &str, key: &str, new_value: &str) -> compact_str::CompactString {
    match raw.find('=') {
        Some(eq) => {
            let after = &raw[eq + 1..];
            let ws_len = after.len() - after.trim_start().len();
            let mut s =
                compact_str::CompactString::with_capacity(eq + 1 + ws_len + new_value.len());
            s.push_str(&raw[..=eq]);
            s.push_str(&after[..ws_len]);
            s.push_str(new_value);
            s
        }
        None => compact_str::format_compact!("{key}={new_value}"),
    }
}

fn parse_ini_path(path: &str) -> (compact_str::CompactString, compact_str::CompactString) {
    let mut section = compact_str::CompactString::default();
    let mut key = compact_str::CompactString::default();
    let mut bracket_depth = 0;
    let mut in_section = true;

    for ch in path.chars() {
        match ch {
            '[' => bracket_depth += 1,
            ']' => bracket_depth -= 1,
            '.' => {
                if bracket_depth > 0 {
                    section.push(ch);
                } else if in_section && !section.is_empty() {
                    in_section = false;
                } else {
                    key.push(ch);
                }
            }
            _ => {
                if in_section {
                    section.push(ch);
                } else {
                    key.push(ch);
                }
            }
        }
    }

    if in_section {
        (compact_str::CompactString::default(), section)
    } else {
        (section, key)
    }
}
