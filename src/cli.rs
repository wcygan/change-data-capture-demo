use clap::{Parser, Subcommand};

#[derive(Debug, Parser, PartialEq, Eq)]
#[command(name = "cdc")]
#[command(about = "Operate the local change data capture teaching demo")]
pub struct CdcArgs {
    #[command(subcommand)]
    pub command: CdcCommand,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum CdcCommand {
    /// Seed the OpenSearch read model with a known user document.
    Seed {
        #[arg(long, default_value_t = 42)]
        user_id: i64,
        #[arg(long, default_value = "Ada")]
        name: String,
        #[arg(long, default_value = "free")]
        plan: String,
    },

    /// Query the current OpenSearch read model for a user.
    Query {
        #[arg(long, default_value_t = 42)]
        user_id: i64,
    },

    /// Produce a Debezium-style user plan update event.
    Produce {
        #[arg(long, default_value_t = 42)]
        user_id: i64,
        #[arg(long, default_value = "Ada")]
        name: String,
        #[arg(long, default_value = "free")]
        from: String,
        #[arg(long, default_value = "pro")]
        to: String,
    },

    /// Delete the OpenSearch read model index.
    Reset,
}

#[derive(Debug, Parser, PartialEq, Eq)]
#[command(name = "indexer")]
#[command(about = "Consume CDC events and update the OpenSearch read model")]
pub struct IndexerArgs {
    #[command(subcommand)]
    pub command: IndexerCommand,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum IndexerCommand {
    /// Run forever, applying each CDC event as it arrives.
    Run,

    /// Consume and apply one CDC event, then exit.
    Once,
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_seed_command() {
        let args = CdcArgs::try_parse_from([
            "cdc",
            "seed",
            "--user-id",
            "42",
            "--name",
            "Ada",
            "--plan",
            "free",
        ])
        .expect("seed command should parse");

        assert_eq!(
            args.command,
            CdcCommand::Seed {
                user_id: 42,
                name: "Ada".to_owned(),
                plan: "free".to_owned()
            }
        );
    }

    #[test]
    fn parses_query_command() {
        let args = CdcArgs::try_parse_from(["cdc", "query", "--user-id", "42"])
            .expect("query command should parse");

        assert_eq!(args.command, CdcCommand::Query { user_id: 42 });
    }

    #[test]
    fn parses_produce_command() {
        let args = CdcArgs::try_parse_from([
            "cdc",
            "produce",
            "--user-id",
            "42",
            "--name",
            "Ada",
            "--from",
            "free",
            "--to",
            "pro",
        ])
        .expect("produce command should parse");

        assert_eq!(
            args.command,
            CdcCommand::Produce {
                user_id: 42,
                name: "Ada".to_owned(),
                from: "free".to_owned(),
                to: "pro".to_owned()
            }
        );
    }

    #[test]
    fn parses_indexer_modes() {
        let run =
            IndexerArgs::try_parse_from(["indexer", "run"]).expect("run command should parse");
        let once =
            IndexerArgs::try_parse_from(["indexer", "once"]).expect("once command should parse");

        assert_eq!(run.command, IndexerCommand::Run);
        assert_eq!(once.command, IndexerCommand::Once);
    }
}
