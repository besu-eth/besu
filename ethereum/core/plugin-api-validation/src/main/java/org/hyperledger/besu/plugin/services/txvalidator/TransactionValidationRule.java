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
package org.hyperledger.besu.plugin.services.txvalidator;

import org.hyperledger.besu.datatypes.Transaction;

import java.util.Optional;

/**
 * A custom transaction validation rule, applied wherever the node validates a transaction: pool
 * admission, block import, block production and simulation. Rules can only be more restrictive, the
 * standard protocol validation still applies. For gating pool admission only, use {@code
 * TransactionPoolValidatorService} instead.
 *
 * <p>Because a rejection at block import makes this node reject blocks that peers without the rule
 * accept, a rule must be applied uniformly across the network.
 *
 * <p>The transaction passed to the rule may not have passed the standard protocol validation, so
 * accessors that recover the sender may throw. The rule is given no indication of which validation
 * path invoked it.
 */
@FunctionalInterface
public interface TransactionValidationRule {
  /**
   * Can be used to validate transactions according to custom needs
   *
   * @param transaction current transaction to validate
   * @return an optional reason if the transaction is invalid or empty if it is valid
   */
  Optional<String> validate(final Transaction transaction);
}
