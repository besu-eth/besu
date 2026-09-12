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
import org.hyperledger.besu.plugin.data.SyncStatus;
import org.hyperledger.besu.plugin.services.Subscription;
import org.hyperledger.besu.plugin.services.sync.SynchronizationService;
import org.hyperledger.besu.plugin.services.sync.spi.InitialSyncCompletionListener;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subscribes to sync status and initial sync completion through {@link SynchronizationService}.
 * The completion subscription closes itself from inside its first callback, exercising
 * unsubscribe-from-callback on the one family that had no remove method on BesuEvents.
 */
@AutoService(BesuPlugin.class)
public class TestSynchronizationSubscriptionPlugin implements BesuPlugin {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSynchronizationSubscriptionPlugin.class);

  private ServiceManager context;
  private Optional<Subscription> syncStatus = Optional.empty();
  private final AtomicReference<Subscription> initialSyncCompletion = new AtomicReference<>();
  private final AtomicInteger syncStatusCounter = new AtomicInteger();
  private File callbackDir;

  @Override
  public void register(final ServiceManager context) {
    this.context = context;
    callbackDir = new File(System.getProperty("besu.plugins.dir", "plugins"));
  }

  @Override
  public void start() {
    final Optional<SynchronizationService> service =
        context.getService(SynchronizationService.class);
    syncStatus = service.map(s -> s.subscribeSyncStatus(this::onSyncStatusChanged));
    service.ifPresent(
        s ->
            initialSyncCompletion.set(
                s.subscribeInitialSyncCompletion(
                    new InitialSyncCompletionListener() {
                      @Override
                      public void onInitialSyncCompleted() {
                        LOG.info("Initial sync completed via subscription");
                        writeCallbackFile("initialSyncCompleted", "completed");
                        final Subscription self = initialSyncCompletion.getAndSet(null);
                        if (self != null) {
                          self.close();
                        }
                      }

                      @Override
                      public void onInitialSyncRestart() {
                        LOG.info("Initial sync restarted via subscription");
                      }
                    })));
    LOG.info("Subscribed to sync status: {}", syncStatus.isPresent());
  }

  @Override
  public void stop() {
    syncStatus.ifPresent(Subscription::close);
    syncStatus = Optional.empty();
    final Subscription completion = initialSyncCompletion.getAndSet(null);
    if (completion != null) {
      completion.close();
    }
  }

  private void onSyncStatusChanged(final Optional<SyncStatus> status) {
    final int count = syncStatusCounter.incrementAndGet();
    LOG.info("Sync status changed via subscription (seen {}) - {}", count, status);
    writeCallbackFile(
        "subscribedSync." + count,
        status.map(s -> Long.toString(s.getCurrentBlock())).orElse("in-sync"));
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
