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
package org.hyperledger.besu.tests.acceptance.plugins;

import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.StartContext;
import org.hyperledger.besu.plugin.services.Subscription;
import org.hyperledger.besu.plugin.services.transactionpool.TxPoolEventService;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Subscribes to transaction-added events through {@link TxPoolEventService}. */
@AutoService(BesuPlugin.class)
public class TestTransactionPoolSubscriptionPlugin implements BesuPlugin {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestTransactionPoolSubscriptionPlugin.class);

  private Optional<Subscription> transactionAdded = Optional.empty();
  private final AtomicInteger transactionCounter = new AtomicInteger();
  private File callbackDir;

  @Override
  public void register(final RegistrationContext context) {
    callbackDir = PluginCallbackDir.resolve(context);
  }

  @Override
  public void start(final StartContext context) {
    transactionAdded =
        Optional.of(
            context
                .getBesuService(TxPoolEventService.class)
                .subscribeTransactionAdded(this::onTransactionAdded));
    LOG.info("Subscribed to transaction added: {}", transactionAdded.isPresent());
  }

  @Override
  public void stop() {
    transactionAdded.ifPresent(Subscription::close);
    transactionAdded = Optional.empty();
  }

  private void onTransactionAdded(final Transaction transaction) {
    final int count = transactionCounter.incrementAndGet();
    LOG.info("Transaction added via subscription (seen {}) - {}", count, transaction.getHash());
    PluginCallbackDir.write(
        callbackDir, "subscribedTx." + count, List.of(transaction.getHash().toHexString()));
  }
}
