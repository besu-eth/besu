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
package org.hyperledger.besu.ethereum.trie.pathbased.common.storage;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

public interface PathBasedLayeredWorldStateKeyValueStorage
    extends PathBasedSnapshotWorldStateKeyValueStorage {

  PathBasedWorldStateKeyValueStorage clone();

  void mergeTo(final SegmentedKeyValueStorageTransaction transaction);

  /** Returns the depth of the underlying LayeredKeyValueStorage chain, or -1 if not applicable. */
  default int layerDepth() {
    return -1;
  }
}
