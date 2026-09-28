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
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.PermissioningService;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;

@AutoService(BesuPlugin.class)
public class TestPermissioningPlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(TestPermissioningPlugin.class);

  private static final String aliceNode =
      "09b02f8a5fddd222ade4ea4528faefc399623af3f736be3c44f03e2df22fb792f3931a4d9573d333ca74343305762a753388c3422a86d98b713fc91c1ea04842";

  private static final String bobNode =
      "af80b90d25145da28c583359beb47b21796b2fe1a23c1511e443e7a64dfdb27d7434c380f0aa4c500e220aa1a9d068514b1ff4d5019e624e7ba1efe82b340a59";

  private static final String charlieNode =
      "ce7edc292d7b747fab2f23584bbafaffde5c8ff17cf689969614441e0527b90015ea9fee96aed6d9c0fc2fbe0bd1883dee223b3200246ff1e21976bdbc9a0fc8";

  PermissioningService service;

  @Override
  public void defineOptions(final PicoCLIOptions options) {
    options.addPicoCLIOptions("permissioning", this);
  }

  @Override
  public void register(final ServiceManager context) {
    service = context.getService(PermissioningService.class).orElseThrow();

    if (enabled) {
      service.registerNodePermissioningProvider(
          (sourceEnode, destinationEnode) -> {
            if (sourceEnode.toString().contains(bobNode)
                || destinationEnode.toString().contains(bobNode)) {

              final boolean isBobTalkingToAlice =
                  sourceEnode.toString().contains(aliceNode)
                      || destinationEnode.toString().contains(aliceNode);
              if (isBobTalkingToAlice) {
                LOG.info("BLOCK CONNECTION from {}, to {}", sourceEnode, destinationEnode);
              } else {
                LOG.info("ALLOW CONNECTION from {}, to {}", sourceEnode, destinationEnode);
              }

              return !isBobTalkingToAlice;
            }
            return true;
          });

      service.registerNodeMessagePermissioningProvider(
          (destinationEnode, code) -> {
            if (destinationEnode.toString().contains(charlieNode) && transactionMessage(code)) {
              LOG.info("BLOCK MESSAGE to {} code {}", destinationEnode, code);
              return false;
            }
            return true;
          });

      service.registerTransactionPermissioningProvider(
          transaction -> {
            long configuredGasLimitThreshold = 22000L;
            long gasLimit = transaction.getGasLimit();
            LOG.info(
                "Transaction gas limit: {} | Configured threshold: {} ",
                gasLimit,
                configuredGasLimitThreshold);
            // Sample logic to testing. If the gas limit is exactly 21000 (intrinsic gas limit)
            // we let the transaction pass. This is helpful for not making additional configuration
            // options in PermssioningPluginTest
            return gasLimit > configuredGasLimitThreshold || gasLimit == 21000;
          });
    }
  }

  @Override
  public void start() {}

  private boolean transactionMessage(final int code) {
    return code == 0x02 || code == 0x08 || code == 0x09 || code == 0x0a;
  }

  @Override
  public void stop() {}

  @Option(names = "--plugin-permissioning-test-enabled")
  boolean enabled = false;
}
