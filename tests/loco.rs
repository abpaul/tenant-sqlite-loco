//! Focused Loco adapter behavior against real tenant files.
use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    routing::get,
    Extension, Router,
};
use loco_rs::{
    app::{AppContext, Initializer},
    environment::Environment,
};
use sea_orm::{ConnectionTrait, Database, DbErr};
use sea_orm_migration::{MigrationName, MigrationTrait, MigratorTrait, SchemaManager};
use serde_json::json;
use tenant_sqlite_loco::{
    Config, Error, Settings, TenantDb, TenantId, TenantInitializer, TenantPools,
};
use tower::ServiceExt;

struct Migrator;
struct CreateRecords;
impl MigrationName for CreateRecords {
    fn name(&self) -> &str {
        "create_records"
    }
}
#[async_trait]
impl MigrationTrait for CreateRecords {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("CREATE TABLE records (value TEXT NOT NULL)")
            .await?;
        Ok(())
    }
}
#[async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(CreateRecords)]
    }
}

fn database_config() -> loco_rs::config::Database {
    serde_json::from_value(json!({
        "uri": "sqlite::memory:", "enable_logging": true,
        "min_connections": 9, "max_connections": 91,
        "connect_timeout": 1234, "idle_timeout": 7, "acquire_timeout": 4321,
        "auto_migrate": false, "dangerously_recreate": true, "dangerously_truncate": true,
        "run_on_start": "PRAGMA application_id = 123"
    }))
    .unwrap()
}

async fn context() -> AppContext {
    let config = serde_json::from_value(json!({
        "logger": {"enable": false, "level": "info", "format": "compact"},
        "server": {"port": 5150, "host": "http://localhost"},
        "database": database_config()
    }))
    .unwrap();
    AppContext::builder(
        Environment::Test,
        Database::connect("sqlite::memory:").await.unwrap(),
        config,
    )
    .build()
}

#[test]
fn typed_settings_preserve_core_limits_and_map_only_connection_settings() {
    let database = database_config();
    let defaults: Settings = serde_json::from_value(json!({"root": "data/tenants"})).unwrap();
    let config = defaults.to_config(&database);
    assert_eq!(config.max_pools, 100);
    assert_eq!(config.max_connections, 2);
    assert_eq!(config.connect_timeout.as_millis(), 1234);
    assert_eq!(config.acquire_timeout.as_millis(), 4321);
    assert_eq!(config.idle_timeout.as_secs(), 60);
    assert_eq!(config.busy_timeout.as_secs(), 5);
    assert_eq!(config.synchronous, tenant_sqlite_loco::Synchronous::Normal);
    assert!(config.sqlx_logging);
    assert_eq!(
        config.run_on_start.as_deref(),
        Some("PRAGMA application_id = 123")
    );
    let overrides: Settings = serde_json::from_value(
        json!({"root": "data/tenants", "max_pools": 3, "max_connections": 1}),
    )
    .unwrap();
    assert_eq!(overrides.to_config(&database).max_pools, 3);
    assert_eq!(overrides.to_config(&database).max_connections, 1);
    assert!(serde_json::from_value::<Settings>(json!({"root": "data", "max_pool": 3})).is_err());
}

async fn read_value(TenantDb(db): TenantDb) -> String {
    db.query_one_raw(sea_orm::Statement::from_string(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT value FROM records".to_owned(),
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get("", "value")
    .unwrap()
}

async fn response(router: Router, id: Option<&TenantId>) -> (StatusCode, String) {
    let mut request = Request::builder().uri("/").body(Body::empty()).unwrap();
    if let Some(id) = id {
        request.extensions_mut().insert(id.clone());
    }
    let response = router.oneshot(request).await.unwrap();
    let status = response.status();
    let body =
        String::from_utf8(to_bytes(response.into_body(), 1024).await.unwrap().to_vec()).unwrap();
    (status, body)
}

#[tokio::test]
async fn initializer_shares_real_databases_with_requests_jobs_and_shutdown() {
    let root = tempfile::tempdir().unwrap();
    let mut config = Config::new(root.path());
    config.max_pools = 2;
    let pools = TenantPools::new::<Migrator>(config).unwrap();
    let ctx = context().await;
    assert!(TenantPools::from_context(&ctx).is_err());
    let initializer = TenantInitializer::new(pools.clone());
    let router = initializer
        .after_routes(Router::new().route("/", get(read_value)), &ctx)
        .await
        .unwrap();
    assert!(pools.install(Router::new(), &ctx).is_err());
    let first = TenantId::new("first").unwrap();
    let second = TenantId::new("second").unwrap();
    let missing = TenantId::new("missing").unwrap();
    assert_eq!(
        response(router.clone(), None).await,
        (
            StatusCode::UNAUTHORIZED,
            "tenant identity required".to_owned()
        )
    );
    assert_eq!(
        response(router.clone(), Some(&missing)).await,
        (
            StatusCode::NOT_FOUND,
            "tenant database not found".to_owned()
        )
    );
    assert!(!root.path().join("missing.db").exists());
    let jobs = TenantPools::from_context(&ctx).unwrap();
    for (id, value) in [(&first, "first value"), (&second, "second value")] {
        let db = jobs.registry().provision(id).await.unwrap();
        db.execute_raw(sea_orm::Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            "INSERT INTO records VALUES (?)",
            [value.into()],
        ))
        .await
        .unwrap();
    }
    assert_eq!(
        response(router.clone(), Some(&first)).await,
        (StatusCode::OK, "first value".to_owned())
    );
    assert_eq!(
        response(router.clone(), Some(&second)).await,
        (StatusCode::OK, "second value".to_owned())
    );
    let third = TenantId::new("third").unwrap();
    drop(jobs.registry().provision(&third).await.unwrap());
    assert_eq!(
        response(Router::new().route("/", get(read_value)), Some(&first)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "tenant database unavailable".to_owned()
        )
    );
    let first_scope = jobs.registry().open_existing(&first).await.unwrap();
    let second_scope = jobs.registry().open_existing(&second).await.unwrap();
    assert_eq!(
        response(router.clone(), Some(&third)).await,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "tenant database unavailable".to_owned()
        )
    );
    assert!(matches!(
        pools.registry().evict(&first).await,
        Err(Error::Capacity)
    ));
    assert_eq!(pools.registry().stats().await.retained_pools, 2);
    drop((first_scope, second_scope));
    tenant_sqlite_loco::shutdown(&ctx).await.unwrap();
    assert_eq!(jobs.registry().stats().await.retained_pools, 0);
    assert_eq!(
        response(router, Some(&first)).await,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "tenant database unavailable".to_owned()
        )
    );
    let restarted = TenantPools::new::<Migrator>(Config::new(root.path())).unwrap();
    let router = Router::new()
        .route("/", get(read_value))
        .layer(Extension(restarted.clone()));
    assert_eq!(
        response(router, Some(&second)).await,
        (StatusCode::OK, "second value".to_owned())
    );
    restarted.registry().shutdown().await.unwrap();
    ctx.db.close().await.unwrap();
}

struct BrokenMigrator;
struct BrokenMigration;
impl MigrationName for BrokenMigration {
    fn name(&self) -> &str {
        "broken"
    }
}
#[async_trait]
impl MigrationTrait for BrokenMigration {
    async fn up(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom("application migration failed".to_owned()))
    }
}
#[async_trait]
impl MigratorTrait for BrokenMigrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(BrokenMigration)]
    }
}

#[tokio::test]
async fn migration_errors_remain_explicit_and_http_responses_hide_details() {
    let root = tempfile::tempdir().unwrap();
    let pools = TenantPools::new::<BrokenMigrator>(Config::new(root.path())).unwrap();
    let id = TenantId::new("broken").unwrap();
    assert!(matches!(
        pools.registry().provision(&id).await,
        Err(Error::Migration(_))
    ));
    assert!(matches!(
        pools.registry().open_existing(&id).await,
        Err(Error::Migration(_))
    ));
    assert_eq!(pools.registry().stats().await.retained_pools, 0);
    let router = Router::new()
        .route("/", get(read_value))
        .layer(Extension(pools.clone()));
    assert_eq!(
        response(router, Some(&id)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "tenant database unavailable".to_owned()
        )
    );
    pools.registry().shutdown().await.unwrap();
}
