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
package org.hyperledger.besu.tests.acceptance.plugins;

import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.Subscription;
import org.hyperledger.besu.plugin.services.transactionpool.TransactionPoolService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Subscribes to transaction-added events through {@link TransactionPoolService}. */
@AutoService(BesuPlugin.class)
public class TestTransactionPoolSubscriptionPlugin implements BesuPlugin {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestTransactionPoolSubscriptionPlugin.class);

  private ServiceManager context;
  private Optional<Subscription> transactionAdded = Optional.empty();
  private final AtomicInteger transactionCounter = new AtomicInteger();
  private File callbackDir;

  @Override
  public void register(final ServiceManager context) {
    this.context = context;
    callbackDir = new File(System.getProperty("besu.plugins.dir", "plugins"));
  }

  @Override
  public void start() {
    transactionAdded =
        context
            .getService(TransactionPoolService.class)
            .map(service -> service.subscribeTransactionAdded(this::onTransactionAdded));
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
    writeCallbackFile("subscribedTx." + count, transaction.getHash().toHexString());
  }

  private void writeCallbackFile(final String name, final String content) {
    try {
      final File callbackFile = new File(callbackDir, name);
      if (!callbackFile.getParentFile().exists()) {
        callbackFile.getParentFile().mkdirs();
        callbackFile.getParentFile().deleteOnExit();
      }
      Files.write(callbackFile.toPath(), Collections.singletonList(content));
      callbackFile.deleteOnExit();
    } catch (final IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
