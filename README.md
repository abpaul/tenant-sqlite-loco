# tenant-sqlite-loco

A thin Loco adapter for [tenant-sqlite](https://github.com/abpaul/tenant-sqlite).
The core owns SQLite files, bounded pools, migrations, scoped access, eviction,
deletion coordination and shutdown. This adapter adds Loco migration wiring,
typed settings, shared-store registration and an optional Axum request extractor.
It targets Loco 1.2 and SeaORM 2.

Inspired by Basecamp's [ActiveRecord::Tenanted](https://github.com/basecamp/activerecord-tenanted)
Rails integration. This is explicit database-per-tenant access, not a complete
port of Rails integrations or Loco's shared-database row-level tenancy helpers.

## Installation

During local development:

```toml
[dependencies]
tenant-sqlite-loco = { path = "../tenant-sqlite-loco" }
```

Core types are reexported, so applications need only the adapter dependency.
The adapter pins its core Git dependency to a commit. For Git dependencies,
replace `path` with `git = "https://github.com/abpaul/tenant-sqlite-loco.git"`
and pin a reviewed full commit SHA using `rev`, or an immutable release `tag`.
Crates.io publication remains disabled.

## Configuration and boot

Deserialize `Settings` within application-owned Loco settings, using Loco's
`Config::settings` helper to report invalid configuration:

```yaml
settings:
  tenants:
    root: data/tenants
    max_pools: 100
    max_connections: 2
```

Create `TenantPools::new::<YourMigrator>(settings.to_config(&ctx.config.database))`
inside your `Hooks::initializers`. Return `TenantInitializer::new(pools)` among
your initializers, or use `pools.install(router, ctx)` in a custom initializer.
Installation happens once during boot and uses the same registry for HTTP and jobs.
No tenant file is opened or provisioned just by installing the service.

`config_from_database` inherits connection/acquisition timeouts, logging and
startup SQL. Control database URI, pool sizes, idle timeout, migration toggles
and destructive recreate/truncate flags are not inherited. Tenant limits and
SQLite durability use core defaults. Set other options on the returned `Config`.
Migrations use Loco's `db::migrate` and run on each pool opening. Failures are
explicit core errors; there is no migration fallback or destructive reset.

## Tenant access

```rust,ignore
use tenant_sqlite_loco::{TenantId, TenantPools};

let pools = TenantPools::from_context(ctx)?;
let id = TenantId::new("customer-123")?;
let created = pools.registry().provision(&id).await?;
drop(created);
let db = pools.registry().open_existing(&id).await?;
// Keep db alive through queries and transaction completion.
drop(db);
```

Provision only from application-owned creation flows. Ordinary requests and
jobs open existing files. Jobs carry explicit tenant IDs; the adapter does not
change queues, schedules, caches or application policy.

The optional `TenantDb` extractor requires middleware to insert `TenantId` into
request extensions **after authentication and authorization**. Never insert an
unchecked host/header ID. A validated ID alone grants no access. Existing
application extractors can call the registry directly instead.

HTTP errors: missing identity → 401; missing database → 404; capacity/shutdown →
503; other failures → generic 500. `Rejection` retains the underlying core error
for application error handling. Responses do not expose internal paths or
migration details. Customize application extractors when another policy is needed.

## Shutdown and deletion

Stop job producers and finish requests, then call `tenant_sqlite_loco::shutdown(ctx)`
from `Hooks::on_shutdown`. It drains the same installed core registry.
For deletion, call `registry.close_for_deletion(&id).await` before application-owned
unlink of the database and sidecars. Keep handles alive through transactions;
do not clone or retain raw database connections outside their scoped handle.

One process and registry own a trusted local root. Active pools count toward
limits. SQLite uses WAL, a five-second busy timeout and synchronous NORMAL by
default. NORMAL can lose recent commits on power loss; configure FULL when
required. The adapter does not change these core guarantees or settings.

See [examples/lifecycle.rs](examples/lifecycle.rs) for a runnable lifecycle example.

## Checks and license

Run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test`.
Three focused tests cover adapter configuration, HTTP/shared-store/lifecycle
integration and migration errors with real temporary SQLite files.

Apache-2.0. Copyright 2026 tenant-sqlite-loco contributors. `publish = false`
until an owner-approved release.
