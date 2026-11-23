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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync2;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.Optional;

/** A head to sync to, with its block and block access list when they came in a new payload. */
record Head(Hash hash, Optional<Block> block, Optional<BlockAccessList> blockAccessList) {

  /** A head the consensus client sent by hash, its block to download. */
  static Head ofHash(final Hash hash) {
    return new Head(hash, Optional.empty(), Optional.empty());
  }

  /** A head the consensus client sent in a new payload. */
  static Head ofPayload(final Block block, final Optional<BlockAccessList> blockAccessList) {
    return new Head(block.getHash(), Optional.of(block), blockAccessList);
  }
}
