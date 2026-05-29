/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import org.immutables.value.Value;

@Value.Immutable
public class SnapSyncConfiguration {

  /**
   * Maximum distance (in blocks) the pivot can lag behind the chain head before {@code
   * PivotSelectorFromSafeBlock} selects a new one. 120 = 128 (snap-serving window) − 8 (≈ 1.5 min
   * buffer at 12 s/slot), so the pivot is replaced while it still has ~1.5 minutes left in the
   * snap-serving window.
   */
  public static final int DEFAULT_PIVOT_BLOCK_WINDOW_VALIDITY = 120;

  /** How often {@code DynamicPivotBlockSelector} re-evaluates whether to refresh the pivot. */
  public static final long DEFAULT_PIVOT_CHECK_INTERVAL_MILLIS = 60_000L;

  public static final int DEFAULT_STORAGE_COUNT_PER_REQUEST =
      384; // The default number of storage entries to download from peers per request.
  public static final int DEFAULT_BYTECODE_COUNT_PER_REQUEST =
      84; // The default number of code entries to download from peers per request.
  public static final int DEFAULT_TRIENODE_COUNT_PER_REQUEST =
      384; // The default number of trienode entries to download from peers per request.

  public static final int DEFAULT_LOCAL_FLAT_ACCOUNT_COUNT_TO_HEAL_PER_REQUEST =
      128; // The default number of flat accounts entries to verify and heal per request.

  public static final int DEFAULT_LOCAL_FLAT_STORAGE_COUNT_TO_HEAL_PER_REQUEST =
      1024; // The default number of flat slots entries to verify and heal per request.

  public static final Boolean DEFAULT_SNAP_SERVER_ENABLED = Boolean.FALSE;
  public static final Boolean DEFAULT_SNAP2_ENABLED = Boolean.FALSE;

  public static final Boolean DEFAULT_SNAP_SYNC_TRANSACTION_INDEXING_ENABLED = Boolean.FALSE;
  public static final Boolean DEFAULT_SNAP_SYNC_SAVE_PRE_MERGE_HEADERS_ONLY_ENABLED = Boolean.TRUE;

  public static SnapSyncConfiguration getDefault() {
    return ImmutableSnapSyncConfiguration.builder().build();
  }

  @Value.Default
  public int getPivotBlockWindowValidity() {
    return DEFAULT_PIVOT_BLOCK_WINDOW_VALIDITY;
  }

  @Value.Default
  public int getStorageCountPerRequest() {
    return DEFAULT_STORAGE_COUNT_PER_REQUEST;
  }

  @Value.Default
  public int getBytecodeCountPerRequest() {
    return DEFAULT_BYTECODE_COUNT_PER_REQUEST;
  }

  @Value.Default
  public int getTrienodeCountPerRequest() {
    return DEFAULT_TRIENODE_COUNT_PER_REQUEST;
  }

  @Value.Default
  public int getLocalFlatAccountCountToHealPerRequest() {
    return DEFAULT_LOCAL_FLAT_ACCOUNT_COUNT_TO_HEAL_PER_REQUEST;
  }

  @Value.Default
  public int getLocalFlatStorageCountToHealPerRequest() {
    return DEFAULT_LOCAL_FLAT_STORAGE_COUNT_TO_HEAL_PER_REQUEST;
  }

  @Value.Default
  public Boolean isSnapServerEnabled() {
    return DEFAULT_SNAP_SERVER_ENABLED;
  }

  @Value.Default
  public Boolean isSnap2Enabled() {
    return DEFAULT_SNAP2_ENABLED;
  }

  @Value.Default
  public Boolean isSnapSyncTransactionIndexingEnabled() {
    return DEFAULT_SNAP_SYNC_TRANSACTION_INDEXING_ENABLED;
  }
}
