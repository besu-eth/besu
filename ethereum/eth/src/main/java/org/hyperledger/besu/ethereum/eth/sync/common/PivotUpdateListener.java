/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.eth.sync.common;

import org.hyperledger.besu.ethereum.core.BlockHeader;

/**
 * Listener interface for pivot block updates. Allows the chain downloader to be notified when the
 * world state downloader updates the pivot block, enabling incremental sync continuation.
 */
public interface PivotUpdateListener {

  /**
   * Called when the pivot block has been updated to a newer block.
   *
   * @param newPivotBlockHeader the new pivot block header
   */
  void onPivotUpdated(BlockHeader newPivotBlockHeader);
}
