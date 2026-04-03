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
package org.hyperledger.besu.ethereum.storage.keyvalue;

import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

/**
 * Routes segments to separate RocksDB instances: {@link #STATE} holds world-state heavy columns so
 * it can use a higher {@code max_open_files} than the rest.
 */
public enum KeyValueStorageSegmentGroup {
  /**
   * Account, code, storage slots, trie nodes, Forest world state, archive/freezer state columns.
   */
  STATE,
  /** Blockchain, trie log, variables, sync segments, legacy columns, etc. */
  MAIN;

  /**
   * Returns {@link #STATE} for world-state storage columns, otherwise {@link #MAIN}.
   *
   * @param segment segment to classify
   */
  public static KeyValueStorageSegmentGroup forSegment(final SegmentIdentifier segment) {
    if (segment instanceof KeyValueSegmentIdentifier id) {
      return forKeyValueSegment(id);
    }
    return MAIN;
  }

  private static KeyValueStorageSegmentGroup forKeyValueSegment(
      final KeyValueSegmentIdentifier id) {
    return switch (id) {
      case ACCOUNT_INFO_STATE,
          CODE_STORAGE,
          ACCOUNT_STORAGE_STORAGE,
          TRIE_BRANCH_STORAGE,
          WORLD_STATE,
          PRUNING_STATE,
          ACCOUNT_INFO_STATE_ARCHIVE,
          ACCOUNT_STORAGE_ARCHIVE,
          ACCOUNT_INFO_STATE_FREEZER,
          ACCOUNT_STORAGE_FREEZER ->
          STATE;
      default -> MAIN;
    };
  }
}
