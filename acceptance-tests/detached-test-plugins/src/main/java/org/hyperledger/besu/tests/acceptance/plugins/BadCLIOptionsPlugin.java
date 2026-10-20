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
import org.hyperledger.besu.plugin.services.PicoCLIOptions;

import java.io.File;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;

@AutoService(BesuPlugin.class)
public class BadCLIOptionsPlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(BadCLIOptionsPlugin.class);

  @Option(names = "--poorly-named-option")
  String poorlyNamedOption = "nothing";

  private File callbackDir;

  @Override
  public void defineOptions(final PicoCLIOptions options) {
    LOG.info("Defining options of BadCliOptionsPlugin");
    if (System.getProperty("TEST_BAD_CLI", "false").equals("true")) {
      options.addPicoCLIOptions("bad-cli", BadCLIOptionsPlugin.this);
    }
  }

  @Override
  public void register(final RegistrationContext context) {
    LOG.info("Registering BadCliOptionsPlugin");
    callbackDir = PluginCallbackDir.resolve(context);
    writeStatus("register");
  }

  @Override
  public void start(final StartContext context) {
    LOG.info("Starting BadCliOptionsPlugin");
    writeStatus("start");
  }

  @Override
  public void stop() {
    LOG.info("Stopping BadCliOptionsPlugin");
    writeStatus("stop");
  }

  private void writeStatus(final String status) {
    PluginCallbackDir.write(callbackDir, "badCLIOptions." + status, status);
    LOG.info("Write status " + status);
  }
}
