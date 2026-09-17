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
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.TransactionPoolValidatorService;

import java.util.Optional;

import com.google.auto.service.AutoService;
import picocli.CommandLine.Option;

@AutoService(BesuPlugin.class)
public class TestTransactionPoolValidatorPlugin2 implements BesuPlugin {

  @Option(names = "--plugin-txpool-validator2-test-enabled")
  boolean enabled = false;

  private ServiceManager serviceManager;

  @Override
  public void register(final ServiceManager serviceManager) {
    this.serviceManager = serviceManager;
    serviceManager
        .getService(PicoCLIOptions.class)
        .orElseThrow()
        .addPicoCLIOptions("txpool-validator2", this);
  }

  @Override
  public void beforeExternalServices() {
    serviceManager
        .getService(TransactionPoolValidatorService.class)
        .orElseThrow()
        .registerPluginTransactionValidatorFactory(
            () ->
                (tx, isLocal, hasPriority) ->
                    enabled && tx.getData().map(bytes -> !bytes.isEmpty()).orElse(false)
                        ? Optional.of("Transaction with payload not allowed here")
                        : Optional.empty());
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}
}
