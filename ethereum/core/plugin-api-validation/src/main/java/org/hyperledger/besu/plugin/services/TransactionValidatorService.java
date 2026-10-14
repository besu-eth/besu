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
package org.hyperledger.besu.plugin.services;

import org.hyperledger.besu.plugin.RegistrationService;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.services.txvalidator.TransactionValidationRule;

/**
 * Service to register additional {@link TransactionValidationRule}s, applied wherever the node
 * validates a transaction.
 *
 * <p>This is a registration-phase service: rules are wired into the node when it is built, after
 * every plugin has returned from {@link org.hyperledger.besu.plugin.BesuPlugin#register}, and a
 * rule registered later is rejected with a {@link
 * org.hyperledger.besu.plugin.RegistrationClosedException}.
 */
@Unstable
public interface TransactionValidatorService extends RegistrationService {

  /**
   * Registers a callback for extending the transaction validation with custom rules.
   *
   * @param rule The rule to register
   * @throws org.hyperledger.besu.plugin.RegistrationClosedException if called after registration
   *     has closed
   */
  void registerTransactionValidatorRule(final TransactionValidationRule rule);
}
