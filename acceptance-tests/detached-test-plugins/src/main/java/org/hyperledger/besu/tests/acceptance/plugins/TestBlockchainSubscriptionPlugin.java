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

import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.AddedBlockContext;
import org.hyperledger.besu.plugin.services.BlockchainService;
import org.hyperledger.besu.plugin.services.Subscription;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Subscribes to block-added events through {@link BlockchainService} rather than BesuEvents. */
@AutoService(BesuPlugin.class)
public class TestBlockchainSubscriptionPlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(TestBlockchainSubscriptionPlugin.class);

  private ServiceManager context;
  private Optional<Subscription> blockAdded = Optional.empty();
  private final AtomicInteger addedBlockCounter = new AtomicInteger();
  private File callbackDir;

  @Override
  public void register(final ServiceManager context) {
    this.context = context;
    callbackDir = new File(System.getProperty("besu.plugins.dir", "plugins"));
  }

  @Override
  public void start() {
    blockAdded =
        context
            .getService(BlockchainService.class)
            .map(service -> service.subscribeBlockAdded(this::onBlockAdded));
    LOG.info("Subscribed to block added: {}", blockAdded.isPresent());
  }

  @Override
  public void stop() {
    blockAdded.ifPresent(Subscription::close);
    blockAdded = Optional.empty();
  }

  private void onBlockAdded(final AddedBlockContext addedBlockContext) {
    final int blockCount = addedBlockCounter.incrementAndGet();
    LOG.info(
        "Block added via subscription (seen {}) - {}",
        blockCount,
        addedBlockContext.getBlockHeader().getNumber());
    writeCallbackFile("subscribedBlock." + blockCount, addedBlockContext.getEventType().name());
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
