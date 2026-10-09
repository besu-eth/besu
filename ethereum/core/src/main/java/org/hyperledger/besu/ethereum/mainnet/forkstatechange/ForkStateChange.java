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
package org.hyperledger.besu.ethereum.mainnet.forkstatechange;

import org.hyperledger.besu.evm.worldstate.WorldUpdater;

/**
 * A state change a fork makes by protocol rule rather than through a transaction, such as the DAO
 * refund (EIP-779). {@link ForkStateChangeProcessor} decides on which block it applies.
 */
@FunctionalInterface
public interface ForkStateChange {

  /**
   * Applies the change. The caller commits the updater.
   *
   * @param updater the updater of the block's world state
   */
  void apply(WorldUpdater updater);
}
