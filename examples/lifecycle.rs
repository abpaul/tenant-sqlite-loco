//! Provision, migrate, access and shut down a tenant database.
use async_trait::async_trait;
use sea_orm::ConnectionTrait;
use sea_orm_migration::{MigrationName, MigrationTrait, MigratorTrait, SchemaManager};
use tenant_sqlite_loco::{Config, TenantId, TenantPools};

struct Migrator;
#[async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(CreateNotes)]
    }
}

struct CreateNotes;
impl MigrationName for CreateNotes {
    fn name(&self) -> &str {
        "create_notes"
    }
}
#[async_trait]
impl MigrationTrait for CreateNotes {
    async fn up(&self, manager: &SchemaManager) -> Result<(), sea_orm::DbErr> {
        manager
            .get_connection()
            .execute_unprepared("CREATE TABLE notes (body TEXT NOT NULL)")
            .await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let mut config = Config::new(root.path());
    config.max_pools = 10;
    config.max_connections = 2;
    let pools = TenantPools::new::<Migrator>(config)?;
    let id = TenantId::new("customer-123")?;
    let created = pools.registry().provision(&id).await?;
    drop(created);
    let db = pools.registry().open_existing(&id).await?;
    db.execute_unprepared("INSERT INTO notes VALUES ('hello')")
        .await?;
    drop(db);
    pools.registry().shutdown().await?;
    Ok(())
}
