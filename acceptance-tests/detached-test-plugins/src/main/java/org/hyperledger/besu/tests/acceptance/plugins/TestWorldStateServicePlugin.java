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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.WorldStateService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@AutoService(BesuPlugin.class)
public class TestWorldStateServicePlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(TestWorldStateServicePlugin.class);
  private ServiceManager serviceManager;
  private File callbackDir;

  @CommandLine.Option(names = "--plugin-world-state-service-test-enabled")
  boolean enabled = false;

  @Override
  public void defineOptions(final PicoCLIOptions options) {
    options.addPicoCLIOptions("world-state-service", this);
  }

  @Override
  public void register(final ServiceManager serviceManager) {
    LOG.info("Registering TestWorldStateServicePlugin");
    this.serviceManager = serviceManager;
    callbackDir = PluginCallbackDir.of(serviceManager);
  }

  @Override
  public void start() {
    if (enabled) {
      LOG.info("Starting TestWorldStateServicePlugin");

      try {
        final var worldStateService =
            serviceManager.getService(WorldStateService.class).orElseThrow();

        final var account =
            worldStateService
                .getWorldView()
                .get(Address.fromHexString("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));
        final var balance = account.getBalance();
        writeBalance(balance);
      } catch (Exception e) {
        LOG.error("Error in TestWorldStateServicePlugin", e);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void stop() {}

  private void writeBalance(final Wei balance) {
    try {
      final File callbackFile = new File(callbackDir, "TestWorldStateServicePlugin.txt");
      if (!callbackFile.getParentFile().exists()) {
        callbackFile.getParentFile().mkdirs();
        callbackFile.getParentFile().deleteOnExit();
      }

      final var content = balance.toHexString();

      Files.write(callbackFile.toPath(), content.getBytes(UTF_8));
      callbackFile.deleteOnExit();
    } catch (final IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
