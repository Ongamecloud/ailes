use chrono::Datelike;
use clap::ArgMatches;
use std::sync::Arc;

const TARGET: &str = env!("CARGO_TARGET");

pub async fn version(
    _matches: &ArgMatches,
    _config: Option<&Arc<crate::config::Config>>,
) -> Result<i32, anyhow::Error> {
    println!(
        "github.com/calagopus/wings {}:{}@{} ({TARGET})",
        crate::VERSION,
        crate::GIT_COMMIT,
        crate::GIT_BRANCH
    );
    if !crate::bins::FUSEQUOTA_VERSION.is_empty() {
        println!(
            "github.com/calagopus/fusequota {} ({} compressed)",
            crate::bins::FUSEQUOTA_VERSION,
            human_bytes::human_bytes(crate::bins::FUSEQUOTA_BIN.len() as f64)
        );
    }
    println!(
        "copyright © 2025 - {} 0x7d8 & Contributors",
        chrono::Local::now().year()
    );

    Ok(0)
}
