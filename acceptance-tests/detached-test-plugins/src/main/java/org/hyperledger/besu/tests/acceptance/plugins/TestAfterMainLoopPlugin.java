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

import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.RunningContext;
import org.hyperledger.besu.plugin.StartContext;
import org.hyperledger.besu.plugin.services.Subscription;
import org.hyperledger.besu.plugin.services.p2p.P2PService;
import org.hyperledger.besu.plugin.services.sync.SyncEventService;
import org.hyperledger.besu.plugin.services.sync.SynchronizationService;
import org.hyperledger.besu.plugin.services.sync.spi.InitialSyncCompletionListener;

import java.io.File;
import java.util.List;

import com.google.auto.service.AutoService;

/**
 * Records in {@code afterMainLoop()} what the running-tier services report and whether the
 * synchronizer had delivered the initial-sync-completion event, which it does from its own start,
 * so the acceptance test can check that the hook runs once the main loop has started.
 */
@AutoService(BesuPlugin.class)
public class TestAfterMainLoopPlugin implements BesuPlugin {
  private File callbackDir;
  private volatile boolean started;
  private volatile boolean initialSyncCompletionSeen;
  private Subscription initialSyncCompletion;

  @Override
  public void register(final RegistrationContext context) {
    callbackDir = PluginCallbackDir.resolve(context);
  }

  @Override
  public void start(final StartContext context) {
    started = true;
    initialSyncCompletion =
        context
            .getBesuService(SyncEventService.class)
            .subscribeInitialSyncCompletion(
                new InitialSyncCompletionListener() {
                  @Override
                  public void onInitialSyncCompleted() {
                    initialSyncCompletionSeen = true;
                  }

                  @Override
                  public void onInitialSyncRestart() {}
                });
  }

  @Override
  public void afterMainLoop(final RunningContext context) {
    final P2PService p2pService = context.getBesuService(P2PService.class);
    final SynchronizationService syncService = context.getBesuService(SynchronizationService.class);
    PluginCallbackDir.write(
        callbackDir,
        "afterMainLoop",
        List.of(
            "started=" + started,
            "p2pEnabled=" + p2pService.isP2pEnabled(),
            "peerCount=" + p2pService.getPeerCount(),
            "initialSyncCompletionSeen=" + initialSyncCompletionSeen,
            "inSync=" + syncService.isInSync()));
  }

  @Override
  public void stop() {
    if (initialSyncCompletion != null) {
      initialSyncCompletion.close();
      initialSyncCompletion = null;
    }
  }
}
