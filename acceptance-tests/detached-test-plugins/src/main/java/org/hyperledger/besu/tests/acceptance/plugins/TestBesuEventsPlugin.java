/*
 * Copyright ConsenSys AG.
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
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.StartContext;
import org.hyperledger.besu.plugin.data.AddedBlockContext;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.data.PropagatedBlockContext;
import org.hyperledger.besu.plugin.services.BesuEvents;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(BesuPlugin.class)
public class TestBesuEventsPlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(TestBesuEventsPlugin.class);

    private BesuEvents besuEvents;
  private Optional<Long> propagationSubscriptionId = Optional.empty();
  private Optional<Long> addedSubscriptionId = Optional.empty();
  private final AtomicInteger propagatedBlockCounter = new AtomicInteger();
  private final AtomicInteger addedBlockCounter = new AtomicInteger();
  private File callbackDir;

  @Override
  public void register(final RegistrationContext context) {
    LOG.info("Registered");
    callbackDir = PluginCallbackDir.resolve(context);
  }

  @Override
  public void start(final StartContext context) {
    besuEvents = context.getBesuService(BesuEvents.class);
    propagationSubscriptionId =
        Optional.of(besuEvents.addBlockPropagatedListener(this::onBlockAnnounce));
    LOG.info("Listening with propagation ID#" + propagationSubscriptionId);
    addedSubscriptionId = Optional.of(besuEvents.addBlockAddedListener(this::onBlockAdded));
    LOG.info("Listening with added ID#" + addedSubscriptionId);
  }

  @Override
  public void stop() {
    propagationSubscriptionId.ifPresent(besuEvents::removeBlockPropagatedListener);
    LOG.info("No longer listening propagation with ID#" + propagationSubscriptionId);
    addedSubscriptionId.ifPresent(besuEvents::removeBlockAddedListener);
    LOG.info("No longer listening added with ID#" + addedSubscriptionId);
  }

  private void onBlockAnnounce(final PropagatedBlockContext propagatedBlockContext) {
    final BlockHeader header = propagatedBlockContext.getBlockHeader();
    final int blockCount = propagatedBlockCounter.incrementAndGet();
    LOG.info("I got a new block! (I've seen {}) - {}", blockCount, header);
    try {
      final File callbackFile = new File(callbackDir, "newBlock." + blockCount);
      if (!callbackFile.getParentFile().exists()) {
        callbackFile.getParentFile().mkdirs();
        callbackFile.getParentFile().deleteOnExit();
      }
      Files.write(callbackFile.toPath(), Collections.singletonList(header.toString()));
      callbackFile.deleteOnExit();
    } catch (final IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private void onBlockAdded(final AddedBlockContext addedBlockContext) {
    final BlockHeader header = addedBlockContext.getBlockHeader();
    final int blockCount = addedBlockCounter.incrementAndGet();
    LOG.info(
        "New block added! (I've seen {}) - {}, eventType {}",
        blockCount,
        header,
        addedBlockContext.getEventType());
    try {
      final File callbackFile = new File(callbackDir, "addedBlock." + blockCount);
      if (!callbackFile.getParentFile().exists()) {
        callbackFile.getParentFile().mkdirs();
        callbackFile.getParentFile().deleteOnExit();
      }
      Files.write(
          callbackFile.toPath(),
          Collections.singletonList(addedBlockContext.getEventType().name()));
      callbackFile.deleteOnExit();
    } catch (final IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
