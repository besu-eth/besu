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
package org.hyperledger.besu.ethereum.eth.transactions.inclusionlist;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;

import java.util.Map;

/**
 * Context for inclusion list validation per EIP-7805. Contains post-execution state needed for the
 * 3-step conditional validation algorithm:
 *
 * <ul>
 *   <li>gasLeft: gas remaining after executing all transactions in the block
 *   <li>accountNonces: post-execution nonce per EOA involved in IL transactions
 *   <li>accountBalances: post-execution balance per EOA involved in IL transactions
 * </ul>
 */
public record InclusionListValidationContext(
    long gasLeft, Map<Address, Long> accountNonces, Map<Address, Wei> accountBalances) {

  /**
   * Creates a context with unlimited gas and empty account state. Used for backward compatibility
   * when full execution context is unavailable. With unlimited gas, only nonce/balance checks (step
   * 3) apply; with empty state, all nonce/balance checks fail (treat sender as having zero balance
   * and nonce 0).
   *
   * @return a context with unlimited gas and empty state
   */
  public static InclusionListValidationContext unlimited() {
    return new InclusionListValidationContext(Long.MAX_VALUE, Map.of(), Map.of());
  }
}
