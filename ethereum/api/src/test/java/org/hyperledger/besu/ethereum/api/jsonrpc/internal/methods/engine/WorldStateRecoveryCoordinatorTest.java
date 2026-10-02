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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Synchronizer;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WorldStateRecoveryCoordinatorTest {
  private static final Hash BLOCK_A = Hash.hash(Bytes32.fromHexStringLenient("0x01"));
  private static final Hash BLOCK_B = Hash.hash(Bytes32.fromHexStringLenient("0x02"));
  private static final Hash STATE_A = Hash.hash(Bytes32.fromHexStringLenient("0x11"));
  private static final Hash STATE_B = Hash.hash(Bytes32.fromHexStringLenient("0x12"));

  @Mock private Synchronizer synchronizer;
  private WorldStateRecoveryCoordinator coordinator;

  @BeforeEach
  void setUp() {
    coordinator = new WorldStateRecoveryCoordinator(synchronizer);
  }

  @Test
  void suppressesDuplicateRecoveryWhileTargetIsActive() {
    when(synchronizer.resyncWorldState()).thenReturn(true);

    coordinator.requestRecovery(BLOCK_A, STATE_A);
    coordinator.requestRecovery(BLOCK_A, STATE_A);

    verify(synchronizer, times(1)).resyncWorldState();
  }

  @Test
  void retriesAfterFailedInitiation() {
    when(synchronizer.resyncWorldState()).thenReturn(false, true);

    coordinator.requestRecovery(BLOCK_A, STATE_A);
    coordinator.requestRecovery(BLOCK_A, STATE_A);

    verify(synchronizer, times(2)).resyncWorldState();
  }

  @Test
  void retriesAfterExceptionDuringInitiation() {
    when(synchronizer.resyncWorldState())
        .thenThrow(new RuntimeException("resync failed"))
        .thenReturn(true);

    coordinator.requestRecovery(BLOCK_A, STATE_A);
    coordinator.requestRecovery(BLOCK_A, STATE_A);

    verify(synchronizer, times(2)).resyncWorldState();
  }

  @Test
  void onlyMatchingTargetCompletesRecovery() {
    when(synchronizer.resyncWorldState()).thenReturn(true);

    coordinator.requestRecovery(BLOCK_A, STATE_A);
    coordinator.markAvailable(BLOCK_B, STATE_B);
    coordinator.requestRecovery(BLOCK_B, STATE_B);
    coordinator.markAvailable(BLOCK_A, STATE_A);
    coordinator.requestRecovery(BLOCK_B, STATE_B);

    verify(synchronizer, times(2)).resyncWorldState();
  }
}
