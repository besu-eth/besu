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
package org.hyperledger.besu.plugin.services.txvalidator;

import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.plugin.Unstable;

import java.util.Optional;

/** Interface for the transaction validator plugin for txpool usage */
@Unstable
public interface PluginTransactionPoolValidator {
  /** Plugin transaction pool validator that unconditionally validates every transaction */
  PluginTransactionPoolValidator VALIDATE_ALL =
      (transaction, isLocal, hasPriority) -> Optional.empty();

  /**
   * Method called to decide whether a transaction can be added to the transaction pool.
   *
   * @param transaction candidate transaction
   * @param isLocal if the transaction was sent to this node via API
   * @param hasPriority if the transaction has priority
   * @return Optional.empty() if the transaction is valid, an Optional containing an error message,
   *     if not
   */
  Optional<String> validateTransaction(
      final Transaction transaction, final boolean isLocal, final boolean hasPriority);
}
