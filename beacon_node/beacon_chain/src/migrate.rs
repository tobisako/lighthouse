use crate::errors::BeaconChainError;
use crate::head_tracker::HeadTracker;
use parking_lot::Mutex;
use slog::{debug, warn, Logger};
use std::collections::{HashMap, HashSet};
use std::mem;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use store::hot_cold_store::{process_finalization, HotColdDBError};
use store::iter::RootsIterator;
use store::{Error, ItemStore, StoreOp};
pub use store::{HotColdDB, MemoryStore};
use types::*;
use types::{BeaconState, BeaconStateHash, EthSpec, Hash256, Slot};

/// Trait for migration processes that update the database upon finalization.
pub trait Migrate<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>>:
    Send + Sync + 'static
{
    fn new(db: Arc<HotColdDB<E, Hot, Cold>>, log: Logger) -> Self;

    fn process_finalization(
        &self,
        _finalized_state_root: BeaconStateHash,
        _new_finalized_state: BeaconState<E>,
        _head_tracker: Arc<HeadTracker>,
        _previous_finalized_checkpoint: Checkpoint,
        _current_finalized_checkpoint: Checkpoint,
    ) {
    }

    /// Traverses live heads and prunes blocks and states of chains that we know can't be built
    /// upon because finalization would prohibit it. This is an optimisation intended to save disk
    /// space.
    ///
    /// Assumptions:
    ///  * It is called after every finalization.
    fn prune_abandoned_forks(
        store: Arc<HotColdDB<E, Hot, Cold>>,
        head_tracker: Arc<HeadTracker>,
        new_finalized_state_hash: BeaconStateHash,
        new_finalized_state: &BeaconState<E>,
        previous_finalized_checkpoint: Checkpoint,
        current_finalized_checkpoint: Checkpoint,
    ) -> Result<(), BeaconChainError> {
        // There will never be any blocks to prune if there is only a single head in the chain.
        if head_tracker.heads().len() == 1 {
            return Ok(());
        }

        assert_eq!(
            new_finalized_state.slot,
            current_finalized_checkpoint
                .epoch
                .start_slot(E::slots_per_epoch())
        );
        eprintln!(
            "Pruning for new finalized checkpoint at epoch: {} (from old {})",
            current_finalized_checkpoint.epoch, previous_finalized_checkpoint.epoch
        );
        eprintln!("new finalized state hash: {:?}", new_finalized_state_hash);

        let old_finalized_slot = previous_finalized_checkpoint
            .epoch
            .start_slot(E::slots_per_epoch());
        let new_finalized_slot = current_finalized_checkpoint
            .epoch
            .start_slot(E::slots_per_epoch());
        println!("Old fin slot: {}", old_finalized_slot);
        println!("New finalized slot: {}", new_finalized_slot);
        let new_finalized_block_hash = current_finalized_checkpoint.root.into();

        // For each slot between the new finalized checkpoint and the old finalized checkpoint,
        // collect the beacon block root and state root of the canonical chain.
        let newly_finalized_chain: HashMap<Slot, (SignedBeaconBlockHash, BeaconStateHash)> =
            std::iter::once(Ok((
                new_finalized_slot,
                (new_finalized_block_hash, new_finalized_state_hash),
            )))
            .chain(
                RootsIterator::new(store.clone(), new_finalized_state).map(|res| {
                    res.map(|(block_root, state_root, slot)| {
                        (slot, (block_root.into(), state_root.into()))
                    })
                }),
            )
            .take_while(|res| {
                res.as_ref()
                    .map_or(true, |(slot, _)| *slot >= old_finalized_slot)
            })
            .collect::<Result<_, _>>()?;

        eprintln!("newly_finalized_chain: {:#?}", newly_finalized_chain);

        // We don't know which blocks are shared among abandoned chains, so we buffer and delete
        // everything in one fell swoop.
        let mut abandoned_blocks: HashSet<SignedBeaconBlockHash> = HashSet::new();
        let mut abandoned_states: HashSet<(Slot, BeaconStateHash)> = HashSet::new();
        let mut abandoned_heads: HashSet<Hash256> = HashSet::new();

        for (head_hash, head_slot) in head_tracker.heads() {
            eprintln!("Pruning head {:?} at slot {}", head_hash, head_slot);
            let mut potentially_abandoned_head: Option<Hash256> = Some(head_hash);
            let mut potentially_abandoned_blocks: Vec<(
                Slot,
                Option<SignedBeaconBlockHash>,
                Option<BeaconStateHash>,
            )> = Vec::new();

            let head_state_hash = store
                .get_block(&head_hash)?
                .ok_or_else(|| BeaconStateError::MissingBeaconBlock(head_hash.into()))?
                .state_root();

            // Iterate backwards from this head, staging blocks and states for deletion.
            let iter = std::iter::once(Ok((head_hash, head_state_hash, head_slot)))
                .chain(RootsIterator::from_block(store.clone(), head_hash)?);
            for maybe_tuple in iter {
                let (block_root, state_root, slot) = maybe_tuple?;

                match newly_finalized_chain.get(&slot) {
                    None => {
                        if slot > new_finalized_slot {
                            eprintln!("Ahead, pruning {:?} (from slot {})", block_root, slot);
                            potentially_abandoned_blocks.push((
                                slot,
                                Some(block_root.into()),
                                Some(state_root.into()),
                            ));
                        } else if slot >= old_finalized_slot {
                            panic!("This shouldn't happen");
                        } else {
                            // We must assume here any candidate chains include old_finalized_block_hash,
                            // i.e. there aren't any forks starting at a block that is a strict ancestor of
                            // old_finalized_block_hash.
                            break;
                        }
                    }
                    Some((finalized_block_root, finalized_state_root)) => {
                        match (
                            block_root == Hash256::from(*finalized_block_root),
                            state_root == Hash256::from(*finalized_state_root),
                        ) {
                            // This fork descends from a newly finalized block.
                            (true, true) => {
                                // If the fork descends from the whole finalized chain,
                                // do not prune it. Otherwise continue to delete all
                                // of the blocks and states that have been staged for
                                // deletion so far.
                                eprintln!(
                                    "Found common ancestor at slot {}: {:?}",
                                    slot, block_root
                                );
                                if slot == new_finalized_slot {
                                    eprintln!("Aborting prune");
                                    potentially_abandoned_blocks.clear();
                                    potentially_abandoned_head.take();
                                }
                                // If there are skipped slots on the fork to be pruned, then
                                // we will have just staged the common block for deletion.
                                // Unstage it.
                                else {
                                    for (_, block_root, _) in
                                        potentially_abandoned_blocks.iter_mut().rev()
                                    {
                                        if block_root.as_ref() == Some(finalized_block_root) {
                                            *block_root = None;
                                        } else {
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                            (true, false) => {
                                eprintln!("block root: {:?}", block_root);
                                eprintln!("state root: {:?}", state_root);
                                eprintln!("finalized state root: {:?}", finalized_state_root);
                                eprintln!("slot: {}", slot);
                                unreachable!("this shouldn't happen")
                            }
                            (false, true) => unreachable!("this shouldn't happen either"),
                            (false, false) => {
                                eprintln!(
                                    "In chain, pruning {:?} (from slot {})",
                                    block_root, slot
                                );
                                potentially_abandoned_blocks.push((
                                    slot,
                                    Some(block_root.into()),
                                    Some(state_root.into()),
                                ));
                            }
                        }
                    }
                }
            }

            abandoned_heads.extend(potentially_abandoned_head.into_iter());
            if !potentially_abandoned_blocks.is_empty() {
                abandoned_blocks.extend(
                    potentially_abandoned_blocks
                        .iter()
                        .filter_map(|(_, maybe_block_hash, _)| *maybe_block_hash),
                );
                abandoned_states.extend(potentially_abandoned_blocks.iter().filter_map(
                    |(slot, _, maybe_state_hash)| match maybe_state_hash {
                        None => None,
                        Some(state_hash) => Some((*slot, *state_hash)),
                    },
                ));
            }
        }

        let batch: Vec<StoreOp<E>> = abandoned_blocks
            .into_iter()
            .map(StoreOp::DeleteBlock)
            .chain(
                abandoned_states
                    .into_iter()
                    .map(|(slot, state_hash)| StoreOp::DeleteState(state_hash, slot)),
            )
            .collect();
        store.do_atomically(batch)?;
        for head_hash in abandoned_heads.into_iter() {
            head_tracker.remove_head(head_hash);
        }

        Ok(())
    }
}

/// Migrator that does nothing, for stores that don't need migration.
pub struct NullMigrator;

impl<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>> Migrate<E, Hot, Cold> for NullMigrator {
    fn new(_: Arc<HotColdDB<E, Hot, Cold>>, _: Logger) -> Self {
        NullMigrator
    }
}

/// Migrator that immediately calls the store's migration function, blocking the current execution.
///
/// Mostly useful for tests.
pub struct BlockingMigrator<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>> {
    db: Arc<HotColdDB<E, Hot, Cold>>,
}

impl<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>> Migrate<E, Hot, Cold>
    for BlockingMigrator<E, Hot, Cold>
{
    fn new(db: Arc<HotColdDB<E, Hot, Cold>>, _: Logger) -> Self {
        BlockingMigrator { db }
    }

    fn process_finalization(
        &self,
        finalized_state_root: BeaconStateHash,
        new_finalized_state: BeaconState<E>,
        head_tracker: Arc<HeadTracker>,
        previous_finalized_checkpoint: Checkpoint,
        current_finalized_checkpoint: Checkpoint,
    ) {
        if let Err(e) = Self::prune_abandoned_forks(
            self.db.clone(),
            head_tracker,
            finalized_state_root.into(),
            &new_finalized_state,
            previous_finalized_checkpoint,
            current_finalized_checkpoint,
        ) {
            eprintln!("Pruning error: {:?}", e);
        }

        if let Err(e) = process_finalization(
            self.db.clone(),
            finalized_state_root.into(),
            &new_finalized_state,
        ) {
            // This migrator is only used for testing, so we just log to stderr without a logger.
            eprintln!("Migration error: {:?}", e);
        }
    }
}

type MpscSender<E> = mpsc::Sender<(
    BeaconStateHash,
    BeaconState<E>,
    Arc<HeadTracker>,
    Checkpoint,
    Checkpoint,
)>;

/// Migrator that runs a background thread to migrate state from the hot to the cold database.
pub struct BackgroundMigrator<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>> {
    db: Arc<HotColdDB<E, Hot, Cold>>,
    tx_thread: Mutex<(MpscSender<E>, thread::JoinHandle<()>)>,
    log: Logger,
}

impl<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>> Migrate<E, Hot, Cold>
    for BackgroundMigrator<E, Hot, Cold>
{
    fn new(db: Arc<HotColdDB<E, Hot, Cold>>, log: Logger) -> Self {
        let tx_thread = Mutex::new(Self::spawn_thread(db.clone(), log.clone()));
        Self { db, tx_thread, log }
    }

    fn process_finalization(
        &self,
        finalized_state_root: BeaconStateHash,
        new_finalized_state: BeaconState<E>,
        head_tracker: Arc<HeadTracker>,
        previous_finalized_checkpoint: Checkpoint,
        current_finalized_checkpoint: Checkpoint,
    ) {
        let (ref mut tx, ref mut thread) = *self.tx_thread.lock();

        if let Err(tx_err) = tx.send((
            finalized_state_root,
            new_finalized_state,
            head_tracker,
            previous_finalized_checkpoint,
            current_finalized_checkpoint,
        )) {
            let (new_tx, new_thread) = Self::spawn_thread(self.db.clone(), self.log.clone());

            *tx = new_tx;
            let old_thread = mem::replace(thread, new_thread);

            // Join the old thread, which will probably have panicked, or may have
            // halted normally just now as a result of us dropping the old `mpsc::Sender`.
            if let Err(thread_err) = old_thread.join() {
                warn!(
                    self.log,
                    "Migration thread died, so it was restarted";
                    "reason" => format!("{:?}", thread_err)
                );
            }

            // Retry at most once, we could recurse but that would risk overflowing the stack.
            let _ = tx.send(tx_err.0);
        }
    }
}

impl<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>> BackgroundMigrator<E, Hot, Cold> {
    /// Spawn a new child thread to run the migration process.
    ///
    /// Return a channel handle for sending new finalized states to the thread.
    fn spawn_thread(
        db: Arc<HotColdDB<E, Hot, Cold>>,
        log: Logger,
    ) -> (MpscSender<E>, thread::JoinHandle<()>) {
        let (tx, rx) = mpsc::channel();
        let thread = thread::spawn(move || {
            while let Ok((
                state_root,
                state,
                head_tracker,
                previous_finalized_checkpoint,
                current_finalized_checkpoint,
            )) = rx.recv()
            {
                match Self::prune_abandoned_forks(
                    db.clone(),
                    head_tracker,
                    state_root,
                    &state,
                    previous_finalized_checkpoint,
                    current_finalized_checkpoint,
                ) {
                    Ok(()) => {}
                    Err(e) => warn!(log, "Block pruning failed: {:?}", e),
                }

                match process_finalization(db.clone(), state_root.into(), &state) {
                    Ok(()) => {}
                    Err(Error::HotColdDBError(HotColdDBError::FreezeSlotUnaligned(slot))) => {
                        debug!(
                            log,
                            "Database migration postponed, unaligned finalized block";
                            "slot" => slot.as_u64()
                        );
                    }
                    Err(e) => {
                        warn!(
                            log,
                            "Database migration failed";
                            "error" => format!("{:?}", e)
                        );
                    }
                };
            }
        });

        (tx, thread)
    }
}
