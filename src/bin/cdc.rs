use std::time::Duration;

use anyhow::Result;
use change_data_capture_demo::cli::{CdcArgs, CdcCommand};
use change_data_capture_demo::config::DemoConfig;
use change_data_capture_demo::connect::ConnectClient;
use change_data_capture_demo::event::UserDocument;
use change_data_capture_demo::search::SearchClient;
use change_data_capture_demo::source::SourceClient;
use clap::Parser;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let args = CdcArgs::parse();
    let config = DemoConfig::from_env();

    run(args, config).await
}

async fn run(args: CdcArgs, config: DemoConfig) -> Result<()> {
    match args.command {
        CdcCommand::Seed {
            user_id,
            name,
            plan,
        } => {
            let source = SourceClient::from_config(&config).await?;
            let search = SearchClient::from_config(&config)?;
            let user = UserDocument::new(user_id, name, plan);

            source.seed_user(&user).await?;
            search.seed_user(&user).await?;

            println!(
                "Seeded source public.users and {}/_doc/{}:",
                config.index, user.id
            );
            println!("{}", serde_json::to_string_pretty(&user)?);
        }
        CdcCommand::Query { user_id } => {
            let search = SearchClient::from_config(&config)?;

            match search.query_user(user_id).await? {
                Some(user) => {
                    println!("Observed {}/_doc/{user_id}:", config.index);
                    println!("{}", serde_json::to_string_pretty(&user)?);
                }
                None => {
                    println!("No document found at {}/_doc/{user_id}.", config.index);
                }
            }
        }
        CdcCommand::Produce {
            user_id,
            name,
            from,
            to,
        } => {
            let source = SourceClient::from_config(&config).await?;
            let user = source.update_user_plan(user_id, &name, &from, &to).await?;

            println!(
                "Updated Postgres source row; Debezium will publish to `{}`:",
                config.topic
            );
            println!("{}", serde_json::to_string_pretty(&user)?);
        }
        CdcCommand::SourceQuery { user_id } => {
            let source = SourceClient::from_config(&config).await?;

            match source.query_user(user_id).await? {
                Some(user) => {
                    println!("Observed source public.users row {user_id}:");
                    println!("{}", serde_json::to_string_pretty(&user)?);
                }
                None => {
                    println!("No source row found in public.users for id {user_id}.");
                }
            }
        }
        CdcCommand::Reset => {
            let source = SourceClient::from_config(&config).await?;
            let search = SearchClient::from_config(&config)?;

            source.reset().await?;
            search.reset().await?;

            println!(
                "Reset source public.users rows and OpenSearch index `{}`.",
                config.index
            );
        }
        CdcCommand::Bootstrap => {
            let source = SourceClient::from_config(&config).await?;
            let connect = ConnectClient::from_config(&config);
            let timeout = Duration::from_millis(config.connector_wait_timeout_ms);

            source.ensure_schema().await?;
            connect.put_postgres_connector(&config).await?;
            let status = connect
                .wait_until_running(&config.connector_name, timeout)
                .await?;

            println!("Registered Debezium connector `{}`:", config.connector_name);
            println!("{}", status.summary());
        }
        CdcCommand::ConnectorStatus => {
            let connect = ConnectClient::from_config(&config);
            let status = connect.status(&config.connector_name).await?;

            println!("Debezium connector `{}`:", config.connector_name);
            println!("{}", status.summary());
        }
        CdcCommand::DeleteConnector => {
            let connect = ConnectClient::from_config(&config);
            connect.delete_connector(&config.connector_name).await?;

            println!(
                "Deleted Debezium connector `{}` if it existed.",
                config.connector_name
            );
        }
    }

    Ok(())
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "change_data_capture_demo=info,cdc=info".into());

    tracing_subscriber::fmt().with_env_filter(filter).init();
}
