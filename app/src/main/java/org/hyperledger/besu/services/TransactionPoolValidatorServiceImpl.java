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
package org.hyperledger.besu.services;

import org.hyperledger.besu.plugin.services.TransactionPoolValidatorService;
import org.hyperledger.besu.plugin.services.txvalidator.PluginTransactionPoolValidator;
import org.hyperledger.besu.plugin.services.txvalidator.PluginTransactionPoolValidatorFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.base.Preconditions;

/** The Transaction pool validator service implementation. */
public class TransactionPoolValidatorServiceImpl implements TransactionPoolValidatorService {

  private final List<PluginTransactionPoolValidatorFactory> factories =
      new CopyOnWriteArrayList<>();

  /** Default Constructor. */
  public TransactionPoolValidatorServiceImpl() {}

  @Override
  public PluginTransactionPoolValidator createTransactionValidator() {
    if (factories.isEmpty()) {
      return PluginTransactionPoolValidator.VALIDATE_ALL;
    }

    return (transaction, isLocal, hasPriority) ->
        factories.stream()
            .map(PluginTransactionPoolValidatorFactory::createTransactionValidator)
            .map(validator -> validator.validateTransaction(transaction, isLocal, hasPriority))
            .filter(Optional::isPresent)
            .findAny()
            .orElse(Optional.empty());
  }

  @Override
  public void registerPluginTransactionValidatorFactory(
      final PluginTransactionPoolValidatorFactory pluginTransactionPoolValidatorFactory) {
    Preconditions.checkNotNull(
        pluginTransactionPoolValidatorFactory,
        "PluginTransactionPoolValidatorFactory must not be null");
    factories.add(pluginTransactionPoolValidatorFactory);
  }
}
