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
import org.hyperledger.besu.plugin.services.p2p.P2PService;
import org.hyperledger.besu.plugin.services.sync.SynchronizationService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import com.google.auto.service.AutoService;

/**
 * Records in {@code afterMainLoop()} what the running-tier services report, so the acceptance test
 * can check that the hook runs once the network and the synchronizer have started.
 */
@AutoService(BesuPlugin.class)
public class TestAfterMainLoopPlugin implements BesuPlugin {
  private File callbackDir;
  private volatile boolean started;

  @Override
  public void register(final RegistrationContext context) {
    callbackDir = PluginCallbackDir.resolve(context);
  }

  @Override
  public void start(final StartContext context) {
    started = true;
  }

  @Override
  public void afterMainLoop(final RunningContext context) {
    final P2PService p2pService = context.getBesuService(P2PService.class);
    final SynchronizationService syncService = context.getBesuService(SynchronizationService.class);
    writeCallbackFile(
        "afterMainLoop",
        List.of(
            "started=" + started,
            "p2pEnabled=" + p2pService.isP2pEnabled(),
            "peerCount=" + p2pService.getPeerCount(),
            "initialSyncPhaseDone=" + syncService.isInitialSyncPhaseDone(),
            "inSync=" + syncService.isInSync()));
  }

  @Override
  public void stop() {}

  private void writeCallbackFile(final String name, final List<String> lines) {
    try {
      final File callbackFile = new File(callbackDir, name);
      if (!callbackFile.getParentFile().exists()) {
        callbackFile.getParentFile().mkdirs();
        callbackFile.getParentFile().deleteOnExit();
      }
      Files.write(callbackFile.toPath(), lines);
      callbackFile.deleteOnExit();
    } catch (final IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
