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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

/** Chooses the JSON-RPC error when a block's world state cannot be loaded. */
final class WorldStateUnavailable {
  private WorldStateUnavailable() {}

  /**
   * Same choice as {@code eth_getProof}. Without the head state the node is still syncing, so the
   * gap is temporary. When the head state is available, the requested state has been pruned.
   */
  static RpcErrorType error(
      final Blockchain blockchain, final WorldStateArchive worldStateArchive) {
    final BlockHeader head = blockchain.getChainHeadHeader();
    return worldStateArchive.isWorldStateAvailable(head.getStateRoot(), head.getHash())
        ? RpcErrorType.PRUNED_HISTORY_UNAVAILABLE
        : RpcErrorType.WORLD_STATE_UNAVAILABLE;
  }
}
