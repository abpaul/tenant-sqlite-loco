//! Loco wiring for the framework-independent `tenant-sqlite` registry.
//!
//! Applications own authentication, tenant resolution and job selection. Stop
//! background producers before calling [`shutdown`] from `Hooks::on_shutdown`.
//! Keep scoped handles alive through queries and transactions.

use std::{path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use axum::{
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Extension, Router,
};
use loco_rs::app::{AppContext, Initializer};
use sea_orm_migration::MigratorTrait;
use serde::Deserialize;

pub use tenant_sqlite::{
    matches_path, Config, Error, MigrationFuture, Registry, Stats, Synchronous, TenantHandle,
    TenantId,
};

/// Create tenant configuration using Loco's connection timeouts, logging and startup SQL.
///
/// Core defaults govern resource limits and durability. Loco's control database
/// URI, pool sizes, idle timeout, automatic migration and destructive flags are
/// deliberately not inherited. Tenant migrations run on every pool opening.
pub fn config_from_database(
    root: impl Into<PathBuf>,
    database: &loco_rs::config::Database,
) -> Config {
    let mut config = Config::new(root);
    config.connect_timeout = Duration::from_millis(database.connect_timeout);
    if let Some(timeout) = database.acquire_timeout {
        config.acquire_timeout = Duration::from_millis(timeout);
    }
    config.sqlx_logging = database.enable_logging;
    config.run_on_start.clone_from(&database.run_on_start);
    config
}

/// Deserialize this within application-owned Loco `settings`.
///
/// Omitted limits use the core defaults. For other options, edit the resulting
/// [`Config`] before constructing [`TenantPools`].
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
#[must_use]
pub struct Settings {
    /// Trusted local root for tenant files.
    pub root: PathBuf,
    /// Hard limit covering all retained pools, including active pools.
    pub max_pools: Option<usize>,
    /// Maximum connections per tenant pool.
    pub max_connections: Option<u32>,
}

impl Settings {
    /// Use core defaults at the supplied root.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            max_pools: None,
            max_connections: None,
        }
    }

    /// Combine typed tenant limits with Loco's connection settings.
    pub fn to_config(&self, database: &loco_rs::config::Database) -> Config {
        let mut config = config_from_database(&self.root, database);
        if let Some(limit) = self.max_pools {
            config.max_pools = limit;
        }
        if let Some(limit) = self.max_connections {
            config.max_connections = limit;
        }
        config
    }
}

/// A cheap shared reference to one core registry, usable by requests and jobs.
#[derive(Clone, Debug)]
#[must_use]
pub struct TenantPools(Arc<Registry>);

impl TenantPools {
    /// Wire application migrations through Loco's supported migration helper.
    ///
    /// # Errors
    /// Returns core configuration or filesystem errors. Tenant migration errors
    /// are returned when provisioning or opening that tenant, not at construction.
    pub fn new<M: MigratorTrait + 'static>(config: Config) -> Result<Self, Error> {
        Registry::new(config, |db| Box::pin(loco_rs::db::migrate::<M>(db)))
            .map(|registry| Self(Arc::new(registry)))
    }

    /// Share an existing core registry with a custom migration hook.
    pub fn from_registry(registry: Arc<Registry>) -> Self {
        Self(registry)
    }

    /// Access core provisioning, opening, deletion, limits and shutdown.
    pub fn registry(&self) -> &Registry {
        &self.0
    }

    /// Register the same service in Loco's shared store and Axum extensions.
    ///
    /// # Errors
    /// Returns a Loco error if a service is already installed. Install once during
    /// application boot; do not register another registry for the same root.
    pub fn install(&self, router: Router, ctx: &AppContext) -> loco_rs::Result<Router> {
        if ctx.shared_store.contains::<Self>() {
            return Err(loco_rs::Error::string("tenant pools are already installed"));
        }
        ctx.shared_store.insert(self.clone());
        Ok(router.layer(Extension(self.clone())))
    }

    /// Retrieve the installed service for explicit tenant access in jobs or tasks.
    ///
    /// # Errors
    /// Returns a Loco error when the initializer has not installed the service.
    pub fn from_context(ctx: &AppContext) -> loco_rs::Result<Self> {
        ctx.shared_store
            .get::<Self>()
            .ok_or_else(|| loco_rs::Error::string("tenant pools are not installed"))
    }
}

/// Installs a supplied service through Loco's initializer hook.
#[derive(Clone, Debug)]
pub struct TenantInitializer(TenantPools);

impl TenantInitializer {
    /// Construct after the application resolves configuration and its migrations.
    #[must_use]
    pub fn new(pools: TenantPools) -> Self {
        Self(pools)
    }
}

#[async_trait]
impl Initializer for TenantInitializer {
    fn name(&self) -> String {
        "tenant-sqlite".to_owned()
    }

    async fn after_routes(&self, router: Router, ctx: &AppContext) -> loco_rs::Result<Router> {
        self.0.install(router, ctx)
    }
}

/// Drain and close the installed registry after requests and job producers stop.
///
/// # Errors
/// Returns a Loco error for missing installation or core closure failure.
/// File removal remains application-owned and follows core `close_for_deletion`.
pub async fn shutdown(ctx: &AppContext) -> loco_rs::Result<()> {
    TenantPools::from_context(ctx)?
        .registry()
        .shutdown()
        .await
        .map_err(loco_rs::Error::wrap)
}

/// Scoped access to an existing tenant database.
///
/// Requires a [`TenantId`] request extension inserted by application middleware
/// **after authorization** and a service installed by [`TenantPools::install`].
/// Validation alone does not authorize an identity. Host/header resolution and
/// authentication remain application-owned. Jobs use the registry directly.
#[derive(Debug)]
#[must_use]
pub struct TenantDb(pub TenantHandle);

/// Extractor failures retain core errors while HTTP responses hide internal details.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Rejection {
    /// Application middleware did not supply an authorized tenant identity.
    #[error("authorized tenant identity is missing")]
    MissingIdentity,
    /// The router does not contain the installed tenant service.
    #[error("tenant pools are not installed")]
    MissingService,
    /// Opening an existing tenant failed.
    #[error(transparent)]
    Database(#[from] Error),
}

impl IntoResponse for Rejection {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::MissingIdentity => (StatusCode::UNAUTHORIZED, "tenant identity required"),
            Self::Database(Error::Missing(_)) => {
                (StatusCode::NOT_FOUND, "tenant database not found")
            }
            Self::Database(Error::Capacity | Error::Shutdown) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "tenant database unavailable",
            ),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "tenant database unavailable",
            ),
        };
        (status, message).into_response()
    }
}

impl<S: Send + Sync> FromRequestParts<S> for TenantDb {
    type Rejection = Rejection;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let id = parts
            .extensions
            .get::<TenantId>()
            .ok_or(Rejection::MissingIdentity)?;
        let pools = parts
            .extensions
            .get::<TenantPools>()
            .ok_or(Rejection::MissingService)?;
        Ok(Self(pools.registry().open_existing(id).await?))
    }
}
