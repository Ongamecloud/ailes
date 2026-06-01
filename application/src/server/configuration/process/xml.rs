use super::ServerConfigurationFile;

pub struct XmlFileParser;

#[async_trait::async_trait]
impl super::ProcessConfigurationFileParser for XmlFileParser {
    async fn process_file(
        content: &str,
        config: &ServerConfigurationFile,
        server: &crate::server::Server,
    ) -> Result<Vec<u8>, anyhow::Error> {
        tracing::debug!(
            server = %server.uuid,
            "processing xml file"
        );

        let content = if content.trim().is_empty() {
            r#"<?xml version="1.0" encoding="UTF-8"?><root></root>"#
        } else {
            content
        };

        let mut root = xmltree::Element::parse(content.as_bytes())?;

        for replacement in &config.replace {
            let value = ServerConfigurationFile::replace_all_placeholders(
                server,
                &replacement.replace_with,
            )
            .await?;

            let path = replacement.r#match.replace('.', "/");
            let path_parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();

            if path.contains('*') {
                update_xml_wildcard(
                    &mut root,
                    &path_parts,
                    &value,
                    replacement.insert_new.unwrap_or(true),
                    replacement.update_existing,
                );
            } else {
                update_xml_element(
                    &mut root,
                    &path_parts,
                    &value,
                    replacement.insert_new.unwrap_or(true),
                    replacement.update_existing,
                );
            }
        }

        let mut result = Vec::new();
        root.write_with_config(
            &mut result,
            xmltree::EmitterConfig::new()
                .perform_indent(true)
                .indent_string("  "),
        )?;

        Ok(result)
    }
}

fn apply_xml_leaf(
    element: &mut xmltree::Element,
    tag: &str,
    value: &str,
    insert_new: bool,
    update_existing: bool,
) {
    if let Some(attr_assignment) = value.strip_prefix('@') {
        let Some((attr_name, attr_val)) = attr_assignment.split_once('=') else {
            return;
        };

        if let Some(child) = element.get_mut_child(tag) {
            let exists = child.attributes.contains_key(attr_name);
            if (exists && update_existing) || (!exists && insert_new) {
                child
                    .attributes
                    .insert(attr_name.to_string(), attr_val.to_string());
            }
        } else if insert_new {
            let mut new_child = xmltree::Element::new(tag);
            new_child
                .attributes
                .insert(attr_name.to_string(), attr_val.to_string());
            element.children.push(xmltree::XMLNode::Element(new_child));
        }
        return;
    }

    if let Some(child) = element.get_mut_child(tag) {
        if update_existing {
            child.children.clear();
            child
                .children
                .push(xmltree::XMLNode::Text(value.to_string()));
        }
    } else if insert_new {
        let mut new_child = xmltree::Element::new(tag);
        new_child
            .children
            .push(xmltree::XMLNode::Text(value.to_string()));
        element.children.push(xmltree::XMLNode::Element(new_child));
    }
}

fn build_xml_chain(
    path: &[&str],
    value: &str,
    insert_new: bool,
    update_existing: bool,
) -> Option<xmltree::Element> {
    let (&last, parents) = path.split_last()?;
    let (&deepest_tag, ancestors) = parents.split_last()?;

    let mut current = xmltree::Element::new(deepest_tag);
    apply_xml_leaf(&mut current, last, value, insert_new, update_existing);

    for &tag in ancestors.iter().rev() {
        let mut parent = xmltree::Element::new(tag);
        parent.children.push(xmltree::XMLNode::Element(current));
        current = parent;
    }

    Some(current)
}

fn update_xml_element(
    element: &mut xmltree::Element,
    path: &[&str],
    value: &str,
    insert_new: bool,
    update_existing: bool,
) {
    let mut element = element;
    let mut path = path;

    loop {
        let (Some(&tag), Some(path_slice)) = (path.first(), path.get(1..)) else {
            return;
        };

        if path.len() == 1 {
            apply_xml_leaf(element, tag, value, insert_new, update_existing);
            return;
        }

        if element.get_mut_child(tag).is_none() {
            if insert_new
                && let Some(new_child) = build_xml_chain(path, value, insert_new, update_existing)
            {
                element.children.push(xmltree::XMLNode::Element(new_child));
            }
            return;
        }

        let Some(child) = element.get_mut_child(tag) else {
            return;
        };

        element = child;
        path = path_slice;
    }
}

fn update_xml_wildcard(
    element: &mut xmltree::Element,
    path: &[&str],
    value: &str,
    insert_new: bool,
    update_existing: bool,
) {
    let mut stack: Vec<(&mut xmltree::Element, &[&str])> = vec![(element, path)];

    while let Some((element, path)) = stack.pop() {
        let Some(&tag) = path.first() else {
            continue;
        };

        let subpath = match path.get(1..) {
            Some(p) if !p.is_empty() => p,
            _ => continue,
        };

        let should_check_insertion = tag != "*" && insert_new;

        let found_match = element.children.iter().any(
            |child| matches!(child, xmltree::XMLNode::Element(e) if tag == "*" || e.name == tag),
        );

        if !found_match {
            if should_check_insertion {
                let mut new_child = xmltree::Element::new(tag);
                if path.len() == 1 {
                    new_child
                        .children
                        .push(xmltree::XMLNode::Text(value.to_string()));
                }
                element.children.push(xmltree::XMLNode::Element(new_child));
            } else {
                continue;
            }
        }

        for child in &mut element.children {
            let xmltree::XMLNode::Element(child_elem) = child else {
                continue;
            };

            if tag != "*" && child_elem.name != tag {
                continue;
            }

            if path.len() == 1 {
                if update_existing {
                    child_elem.children.clear();
                    child_elem
                        .children
                        .push(xmltree::XMLNode::Text(value.to_string()));
                }
            } else {
                stack.push((child_elem, subpath));
            }
        }
    }
}
