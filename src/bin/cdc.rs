use anyhow::Result;
use change_data_capture_demo::cli::{CdcArgs, CdcCommand};
use change_data_capture_demo::config::DemoConfig;
use change_data_capture_demo::event::{DebeziumUpdateEvent, UserDocument};
use change_data_capture_demo::kafka::produce_event;
use change_data_capture_demo::search::SearchClient;
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
            let search = SearchClient::from_config(&config)?;
            let user = UserDocument::new(user_id, name, plan);

            search.seed_user(&user).await?;

            println!("Seeded {}/_doc/{}:", config.index, user.id);
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
            let event =
                DebeziumUpdateEvent::user_plan_update(&config.topic, user_id, name, from, to);

            produce_event(&config, &event).await?;

            println!("Produced CDC update event to `{}`:", config.topic);
            println!("{}", serde_json::to_string_pretty(&event)?);
        }
        CdcCommand::Reset => {
            let search = SearchClient::from_config(&config)?;
            search.reset().await?;

            println!("Reset OpenSearch index `{}`.", config.index);
        }
    }

    Ok(())
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "change_data_capture_demo=info,cdc=info".into());

    tracing_subscriber::fmt().with_env_filter(filter).init();
}
