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
package org.hyperledger.besu.ethereum.mainnet.block.access.list;

import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;

/**
 * Factory for creating Block Access List builders. Only present when BAL is activated (e.g.
 * Amsterdam and later); when not a BAL fork, the protocol spec has an empty {@code
 * Optional<BlockAccessListFactory>}.
 */
public class BlockAccessListFactory {

  public BlockAccessListFactory() {}

  /** Returns a new builder for tracking block access list data. */
  public BlockAccessListBuilder newBlockAccessListBuilder() {
    return BlockAccessList.builder();
  }
}
