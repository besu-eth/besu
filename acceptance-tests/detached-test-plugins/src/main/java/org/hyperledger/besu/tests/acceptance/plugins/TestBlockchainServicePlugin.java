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

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.BesuEvents;
import org.hyperledger.besu.plugin.services.BlockchainService;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@AutoService(BesuPlugin.class)
public class TestBlockchainServicePlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(TestBlockchainServicePlugin.class);
  private final List<HardforkSeen> seenHardforks = new ArrayList<>();
  private ServiceManager serviceManager;
  private File callbackDir;

  @CommandLine.Option(names = "--plugin-blockchain-service-test-enabled")
  boolean enabled = false;

  @Override
  public void defineOptions(final PicoCLIOptions options) {
    options.addPicoCLIOptions("blockchain-service", this);
  }

  @Override
  public void register(final ServiceManager serviceManager) {
    LOG.info("Registering TestBlockchainServicePlugin");
    this.serviceManager = serviceManager;

    callbackDir = PluginCallbackDir.of(serviceManager);
  }

  @Override
  public void start() {
    if (enabled) {
      LOG.info("Starting TestBlockchainServicePlugin");
      final var blockchainService =
          serviceManager.getService(BlockchainService.class).orElseThrow();

      serviceManager
          .getService(BesuEvents.class)
          .orElseThrow()
          .addBlockAddedListener(
              addedBlockContext -> {
                LOG.info("Block added: {}", addedBlockContext);
                final var hardforkSeen =
                    queryHardfork(blockchainService, addedBlockContext.getBlockHeader());
                seenHardforks.add(
                    queryHardfork(blockchainService, addedBlockContext.getBlockHeader()));
                if (hardforkSeen.current.equals(HardforkId.MainnetHardforkId.LONDON)) {
                  LOG.info("Writing seen hardforks: {}", seenHardforks);
                  writeSeenHardforks();
                }
              });

      seenHardforks.add(queryHardfork(blockchainService, blockchainService.getChainHeadHeader()));
    }
  }

  private HardforkSeen queryHardfork(
      final BlockchainService blockchainService, final BlockHeader header) {
    final var currentHardfork = blockchainService.getHardforkId(header);
    final var currentHardforkByNumber = blockchainService.getHardforkId(header.getNumber());
    final var nextHardfork =
        blockchainService.getNextBlockHardforkId(header, header.getTimestamp() + 1);

    return new HardforkSeen(
        header.getNumber(), currentHardfork, currentHardforkByNumber, nextHardfork);
  }

  @Override
  public void stop() {}

  private void writeSeenHardforks() {
    try {
      final File callbackFile = new File(callbackDir, "hardfork.list");
      if (!callbackFile.getParentFile().exists()) {
        callbackFile.getParentFile().mkdirs();
        callbackFile.getParentFile().deleteOnExit();
      }

      final var content =
          seenHardforks.stream()
              .map(
                  r ->
                      String.join(
                          ",",
                          String.valueOf(r.blockNumber),
                          r.current.name(),
                          r.currentByNumber().name(),
                          r.next.name()))
              .collect(Collectors.joining("\n"));

      Files.write(callbackFile.toPath(), content.getBytes(UTF_8));
      callbackFile.deleteOnExit();
    } catch (final IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private record HardforkSeen(
      long blockNumber, HardforkId current, HardforkId currentByNumber, HardforkId next) {}
}
