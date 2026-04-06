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

/**
 * Listener interface for world state heal finished notifications. Allows the chain downloader to be
 * notified when the world state heal download has finished and will not select any new pivot
 * blocks, enabling safe completion of chain download without missing pivot updates.
 */
public interface WorldStateHealFinishedListener {

  /**
   * Called when the world state heal has finished and will not select any new pivot blocks. This
   * signals that it is safe for the chain downloader to complete.
   */
  void onWorldStateHealFinished();
}
