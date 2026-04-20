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
package org.hyperledger.besu.ethereum.mainnet;

/**
 * Outcome of {@link BlockAccessListValidator#validateExecutedBlockAccessListItemSize} (EIP-7928
 * item budget vs gas limit).
 */
public sealed interface BlockAccessListItemSizeCheck
    permits BlockAccessListItemSizeCheck.WithinBudget, BlockAccessListItemSizeCheck.OverBudget {

  record WithinBudget() implements BlockAccessListItemSizeCheck {}

  record OverBudget(BlockAccessListValidationError error) implements BlockAccessListItemSizeCheck {}

  static WithinBudget withinBudget() {
    return new WithinBudget();
  }

  static OverBudget overBudget(final BlockAccessListValidationError error) {
    return new OverBudget(error);
  }
}
