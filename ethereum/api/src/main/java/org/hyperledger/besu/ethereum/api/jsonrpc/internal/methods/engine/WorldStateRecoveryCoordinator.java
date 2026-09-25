/*
 * Copyright contributors to Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Synchronizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Coordinates FCU-triggered world-state recovery across all Engine API versions. */
public final class WorldStateRecoveryCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(WorldStateRecoveryCoordinator.class);

  private final Synchronizer synchronizer;
  private RecoveryTarget activeTarget;
  private long nextGeneration;

  public WorldStateRecoveryCoordinator(final Synchronizer synchronizer) {
    this.synchronizer = synchronizer;
  }

  /** Creates a coordinator that is inert for tests or methods without a synchronizer. */
  public static WorldStateRecoveryCoordinator disabled() {
    return new WorldStateRecoveryCoordinator(null);
  }

  /** Clears recovery only when the observed available state matches the active target. */
  public synchronized void markAvailable(final Hash blockHash, final Hash stateRoot) {
    if (activeTarget != null && activeTarget.matches(blockHash, stateRoot)) {
      activeTarget = null;
      LOG.debug("World-state recovery completed for block {}", blockHash);
    }
  }

  /** Starts one recovery attempt and suppresses duplicate attempts while it is active. */
  public void requestRecovery(final Hash blockHash, final Hash stateRoot) {
    final RecoveryTarget target;
    synchronized (this) {
      if (synchronizer == null || activeTarget != null) {
        if (activeTarget != null) {
          LOG.debug(
              "Suppressing duplicate world-state recovery for block {} while recovery for {} is active",
              blockHash,
              activeTarget.blockHash());
        }
        return;
      }
      target = new RecoveryTarget(blockHash, stateRoot, ++nextGeneration);
      activeTarget = target;
      LOG.info("Starting world-state recovery for block {}", blockHash);
    }

    try {
      if (synchronizer.resyncWorldState()) {
        LOG.debug("World-state recovery initiated for block {}", blockHash);
      } else {
        clearAfterFailedInitiation(target);
        LOG.warn("World-state recovery could not be initiated for block {}", blockHash);
      }
    } catch (final RuntimeException e) {
      clearAfterFailedInitiation(target);
      LOG.warn("World-state recovery failed to start for block {}", blockHash, e);
    }
  }

  private synchronized void clearAfterFailedInitiation(final RecoveryTarget target) {
    if (target.equals(activeTarget)) {
      activeTarget = null;
    }
  }

  private record RecoveryTarget(Hash blockHash, Hash stateRoot, long generation) {
    private boolean matches(final Hash otherBlockHash, final Hash otherStateRoot) {
      return blockHash.equals(otherBlockHash) && stateRoot.equals(otherStateRoot);
    }
  }
}
