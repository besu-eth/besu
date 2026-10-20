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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.StartContext;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.WorldStateService;

import java.io.File;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@AutoService(BesuPlugin.class)
public class TestWorldStateServicePlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(TestWorldStateServicePlugin.class);
  private File callbackDir;

  @CommandLine.Option(names = "--plugin-world-state-service-test-enabled")
  boolean enabled = false;

  @Override
  public void defineOptions(final PicoCLIOptions options) {
    options.addPicoCLIOptions("world-state-service", this);
  }

  @Override
  public void register(final RegistrationContext context) {
    LOG.info("Registering TestWorldStateServicePlugin");
    callbackDir = PluginCallbackDir.resolve(context);
  }

  @Override
  public void start(final StartContext context) {
    if (enabled) {
      LOG.info("Starting TestWorldStateServicePlugin");

      try {
        final var worldStateService = context.getBesuService(WorldStateService.class);

        final var account =
            worldStateService
                .getWorldView()
                .get(Address.fromHexString("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));
        final var balance = account.getBalance();
        PluginCallbackDir.write(
            callbackDir, "TestWorldStateServicePlugin.txt", balance.toHexString());
      } catch (Exception e) {
        LOG.error("Error in TestWorldStateServicePlugin", e);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void stop() {}
}
