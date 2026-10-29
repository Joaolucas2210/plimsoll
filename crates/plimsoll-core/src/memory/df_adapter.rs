//! DataFusion adapter: `CeilingPool` *is* the session's `MemoryPool`.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};

use super::{CeilingError, CeilingPool, ConsumerKind};

impl From<CeilingError> for DataFusionError {
    fn from(e: CeilingError) -> Self {
        DataFusionError::ResourcesExhausted(e.to_string())
    }
}

impl MemoryPool for CeilingPool {
    fn name(&self) -> &str {
        "plimsoll-ceiling"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        // ponytail: DataFusion consumers are only Spillable/NonSpillable; its
        // exchange (RepartitionExec) stays in the working budget. The Output
        // slice is for Plimsoll's own result/exchange buffers (native API).
        let kind = if consumer.can_spill() {
            ConsumerKind::Spillable
        } else {
            ConsumerKind::NonSpillable
        };
        self.register_id(consumer.id(), consumer.name().to_owned(), kind);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.unregister_id(consumer.id());
    }

    /// Infallible by DataFusion's contract: accounted but unchecked, so the
    /// overshoot is visible and blocks every later `try_grow`.
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.grow_id(reservation.consumer().id(), additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.shrink_id(reservation.consumer().id(), shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        Ok(self.try_grow_id(reservation.consumer().id(), additional)?)
    }

    fn reserved(&self) -> usize {
        self.used()
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.limit)
    }
}

/// Runtime whose every reservation goes through `pool`.
/// `spill_dir = None` disables spilling (operators fail instead of spilling).
pub fn runtime_env(pool: Arc<CeilingPool>, spill_dir: Option<PathBuf>) -> Result<Arc<RuntimeEnv>> {
    let mode = match spill_dir {
        Some(dir) => DiskManagerMode::Directories(vec![dir]),
        None => DiskManagerMode::Disabled,
    };
    RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(mode))
        .build_arc()
}

/// `SessionContext` running under the ceiling. Share one `pool` across all
/// sessions of the process — that is what makes it a process ceiling.
pub fn session_context_with_ceiling(
    pool: Arc<CeilingPool>,
    spill_dir: Option<PathBuf>,
    config: SessionConfig,
) -> Result<SessionContext> {
    Ok(SessionContext::new_with_config_rt(
        config,
        runtime_env(pool, spill_dir)?,
    ))
}

/// Total bytes spilled by an executed plan (sum over the whole tree).
pub fn spilled_bytes(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let own = plan.metrics().and_then(|m| m.spilled_bytes()).unwrap_or(0);
    own + plan
        .children()
        .into_iter()
        .map(spilled_bytes)
        .sum::<usize>()
}
