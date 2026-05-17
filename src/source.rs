use anyhow::{Context, Result, bail};
use tokio_postgres::{Client, NoTls};

use crate::config::DemoConfig;
use crate::event::UserDocument;

pub struct SourceClient {
    client: Client,
}

impl SourceClient {
    pub async fn from_config(config: &DemoConfig) -> Result<Self> {
        let (client, connection) =
            tokio_postgres::connect(&config.postgres_connection_string(), NoTls)
                .await
                .context("failed to connect to Postgres source database")?;

        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(%error, "Postgres source connection failed");
            }
        });

        Ok(Self { client })
    }

    pub async fn ensure_schema(&self) -> Result<()> {
        self.client
            .batch_execute(
                r#"
                CREATE TABLE IF NOT EXISTS public.users (
                    id BIGINT PRIMARY KEY,
                    name TEXT NOT NULL,
                    plan TEXT NOT NULL
                );

                ALTER TABLE public.users REPLICA IDENTITY FULL;
                "#,
            )
            .await
            .context("failed to ensure source users table")
    }

    pub async fn seed_user(&self, user: &UserDocument) -> Result<()> {
        self.ensure_schema().await?;

        self.client
            .execute("DELETE FROM public.users WHERE id = $1", &[&user.id])
            .await
            .context("failed to delete existing source user before seed")?;

        self.client
            .execute(
                "INSERT INTO public.users (id, name, plan) VALUES ($1, $2, $3)",
                &[&user.id, &user.name, &user.plan],
            )
            .await
            .context("failed to insert source user seed row")?;

        Ok(())
    }

    pub async fn update_user_plan(
        &self,
        user_id: i64,
        name: &str,
        expected_plan: &str,
        new_plan: &str,
    ) -> Result<UserDocument> {
        self.ensure_schema().await?;

        let updated_rows = self
            .client
            .execute(
                r#"
                UPDATE public.users
                SET name = $2, plan = $3
                WHERE id = $1 AND plan = $4
                "#,
                &[&user_id, &name, &new_plan, &expected_plan],
            )
            .await
            .context("failed to update source user plan")?;

        if updated_rows == 0 {
            let current = self.query_user(user_id).await?;
            bail!(
                "source user {user_id} was not updated from `{expected_plan}` to `{new_plan}`; current row: {current:?}"
            );
        }

        Ok(UserDocument::new(user_id, name, new_plan))
    }

    pub async fn query_user(&self, user_id: i64) -> Result<Option<UserDocument>> {
        self.ensure_schema().await?;

        let row = self
            .client
            .query_opt(
                "SELECT id, name, plan FROM public.users WHERE id = $1",
                &[&user_id],
            )
            .await
            .context("failed to query source user")?;

        Ok(row.map(|row| {
            UserDocument::new(
                row.get::<_, i64>("id"),
                row.get::<_, String>("name"),
                row.get::<_, String>("plan"),
            )
        }))
    }

    pub async fn reset(&self) -> Result<()> {
        self.ensure_schema().await?;

        self.client
            .execute("DELETE FROM public.users", &[])
            .await
            .context("failed to reset source users table")?;

        Ok(())
    }
}
