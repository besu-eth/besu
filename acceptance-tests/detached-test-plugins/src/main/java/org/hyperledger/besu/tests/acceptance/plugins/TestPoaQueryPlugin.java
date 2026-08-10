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
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.AddedBlockContext;
import org.hyperledger.besu.plugin.services.BesuEvents;
import org.hyperledger.besu.plugin.services.query.BftQueryService;
import org.hyperledger.besu.plugin.services.query.PoaQueryService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reports what {@link PoaQueryService} and {@link BftQueryService} answer on a running node, so an
 * acceptance test can assert the services are registered and consistent. On networks where the
 * services are not registered (anything that is not IBFT2 or QBFT) the plugin stays passive.
 */
@AutoService(BesuPlugin.class)
public class TestPoaQueryPlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(TestPoaQueryPlugin.class);

  private ServiceManager serviceManager;
  private File callbackDir;
  private Optional<Long> subscriptionId = Optional.empty();
  private final AtomicBoolean reported = new AtomicBoolean(false);

  @Override
  public void register(final ServiceManager serviceManager) {
    LOG.info("Registering TestPoaQueryPlugin");
    this.serviceManager = serviceManager;
    callbackDir = new File(System.getProperty("besu.plugins.dir", "plugins"));
  }

  @Override
  public void start() {
    if (serviceManager.getService(PoaQueryService.class).isEmpty()
        || serviceManager.getService(BftQueryService.class).isEmpty()) {
      LOG.info("PoaQueryService or BftQueryService not registered, TestPoaQueryPlugin is passive");
      return;
    }
    subscriptionId =
        serviceManager
            .getService(BesuEvents.class)
            .map(events -> events.addBlockAddedListener(this::onBlockAdded));
    LOG.info("Listening for added blocks with ID#" + subscriptionId);
  }

  @Override
  public void stop() {
    subscriptionId.ifPresent(
        id ->
            serviceManager
                .getService(BesuEvents.class)
                .ifPresent(events -> events.removeBlockAddedListener(id)));
  }

  private void onBlockAdded(final AddedBlockContext addedBlockContext) {
    if (reported.getAndSet(true)) {
      return;
    }
    final PoaQueryService poaQueryService =
        serviceManager.getService(PoaQueryService.class).orElseThrow();
    final BftQueryService bftQueryService =
        serviceManager.getService(BftQueryService.class).orElseThrow();

    final var header = addedBlockContext.getBlockHeader();
    final String report =
        String.join(
            "\n",
            "mechanism=" + bftQueryService.getConsensusMechanismName(),
            "localSigner=" + poaQueryService.getLocalSignerAddress(),
            "validators=" + toCsv(poaQueryService.getValidatorsForLatestBlock()),
            "proposer=" + poaQueryService.getProposerOfBlock(header),
            "round=" + bftQueryService.getRoundNumberFrom(header),
            "signers=" + toCsv(bftQueryService.getSignersFrom(header)),
            "sameInstance=" + (poaQueryService == bftQueryService));
    writeReport(report);
  }

  private static String toCsv(final Collection<Address> addresses) {
    return addresses.stream().map(Address::toString).collect(Collectors.joining(","));
  }

  private void writeReport(final String report) {
    try {
      final File callbackFile = new File(callbackDir, "poaQueryService.report");
      if (!callbackFile.getParentFile().exists()) {
        callbackFile.getParentFile().mkdirs();
        callbackFile.getParentFile().deleteOnExit();
      }
      Files.writeString(callbackFile.toPath(), report);
      callbackFile.deleteOnExit();
    } catch (final IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
