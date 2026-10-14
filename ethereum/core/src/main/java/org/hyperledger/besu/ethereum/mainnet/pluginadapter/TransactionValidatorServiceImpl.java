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
package org.hyperledger.besu.ethereum.mainnet.pluginadapter;

import org.hyperledger.besu.plugin.RegistrationClosedException;
import org.hyperledger.besu.plugin.services.TransactionValidatorService;
import org.hyperledger.besu.plugin.services.txvalidator.TransactionValidationRule;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The transaction validator service implementation. */
public class TransactionValidatorServiceImpl implements TransactionValidatorService {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionValidatorServiceImpl.class);

  private final List<TransactionValidationRule> transactionValidationRules = new ArrayList<>();
  private final BooleanSupplier registrationOpen;

  /** Creates a service that accepts rules at any time. */
  public TransactionValidatorServiceImpl() {
    this(() -> true);
  }

  /**
   * Creates the service.
   *
   * @param registrationOpen whether plugin registration is currently open; rules registered while
   *     it is closed are rejected
   */
  public TransactionValidatorServiceImpl(final BooleanSupplier registrationOpen) {
    this.registrationOpen = registrationOpen;
  }

  @Override
  public void registerTransactionValidatorRule(final TransactionValidationRule rule) {
    if (!registrationOpen.getAsBoolean()) {
      throw new RegistrationClosedException(
          "TransactionValidatorService", "registerTransactionValidatorRule");
    }
    transactionValidationRules.add(rule);
    LOG.info("Registered new transaction validator rule");
  }

  /**
   * Gets transaction validator rules.
   *
   * @return the transaction validator rules
   */
  public List<TransactionValidationRule> getTransactionValidatorRules() {
    return transactionValidationRules;
  }
}
