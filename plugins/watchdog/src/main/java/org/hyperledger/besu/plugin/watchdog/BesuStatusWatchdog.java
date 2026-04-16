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
package org.hyperledger.besu.plugin.watchdog;

import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.AddedBlockContext;
import org.hyperledger.besu.plugin.services.BesuEvents;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.RpcEndpointService;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * A novel Besu plugin that monitors blockchain status and provides an RPC overview.
 * This plugin serves as a demonstration of the Plugin API capabilities.
 */
public class BesuStatusWatchdog implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(BesuStatusWatchdog.class);

  private ServiceManager context;
  private BesuEvents besuEvents;
  private RpcEndpointService rpcEndpointService;

  private final AtomicLong blocksSeen = new AtomicLong(0);
  private final AtomicLong totalTransactionsSeen = new AtomicLong(0);
  private final AtomicLong totalGasUsed = new AtomicLong(0);

  private boolean enabled = false;
  private long statsHistoryLimit = 100;

  @Override
  public void register(final ServiceManager context) {
    this.context = context;
    LOG.info("Registering Watchdog Plugin");

    // Register CLI options
    context.getService(PicoCLIOptions.class).ifPresent(picoCLIOptions -> {
      picoCLIOptions.addPicoCLIOptions("watchdog", this);
    });
  }

  @Override
  public void start() {
    if (!enabled) {
      LOG.info("Watchdog Plugin is disabled. Use --plugin-watchdog-enabled to enable it.");
      return;
    }

    LOG.info("Starting Watchdog Plugin (History Limit: {})", statsHistoryLimit);

    // Get required services
    besuEvents = context.getService(BesuEvents.class).orElseThrow(() -> 
        new RuntimeException("BesuEvents service is not available"));
    
    rpcEndpointService = context.getService(RpcEndpointService.class).orElseThrow(() -> 
        new RuntimeException("RpcEndpointService is not available"));

    // Register Block Added Listener
    besuEvents.addBlockAddedListener(this::onBlockAdded);

    // Register RPC methods
    rpcEndpointService.registerRPCEndpoint("watchdog", "getOverview", (request) -> {
      return new WatchdogOverview(
          blocksSeen.get(),
          totalTransactionsSeen.get(),
          totalGasUsed.get(),
          blocksSeen.get() > 0 ? (double) totalGasUsed.get() / blocksSeen.get() : 0.0
      );
    });
  }

  @Override
  public void stop() {
    LOG.info("Stopping Watchdog Plugin. Summary: Seen {} blocks, {} transactions.", 
             blocksSeen.get(), totalTransactionsSeen.get());
  }

  private void onBlockAdded(final AddedBlockContext addedBlockContext) {
    blocksSeen.incrementAndGet();
    
    addedBlockContext.getBlock().getBody().ifPresent(body -> {
      int txCount = body.getTransactions().size();
      totalTransactionsSeen.addAndGet(txCount);
      
      long gasUsed = addedBlockContext.getBlock().getHeader().getGasUsed();
      totalGasUsed.addAndGet(gasUsed);

      LOG.debug("Watchdog: Block {} added. Txs: {}, Gas: {}", 
                addedBlockContext.getBlock().getHeader().getNumber(), txCount, gasUsed);
    });
  }

  @CommandLine.Option(
      names = {"--plugin-watchdog-enabled"},
      description = "Enable the Watchdog plugin (default: ${DEFAULT-VALUE})",
      arity = "1")
  public void setEnabled(final boolean enabled) {
    this.enabled = enabled;
  }

  @CommandLine.Option(
      names = {"--plugin-watchdog-max-stats-history"},
      description = "Maximum number of blocks to keep in history for stats (default: ${DEFAULT-VALUE})",
      arity = "1")
  public void setStatsHistoryLimit(final long statsHistoryLimit) {
    this.statsHistoryLimit = statsHistoryLimit;
  }

  @Override
  public String getName() {
    return "watchdog";
  }

  /**
   * Data class for RPC response
   */
  public static class WatchdogOverview {
    public final long blocksSeen;
    public final long totalTransactions;
    public final long totalGasUsed;
    public final double averageGasPerBlock;

    public WatchdogOverview(long blocksSeen, long totalTransactions, long totalGasUsed, double averageGasPerBlock) {
      this.blocksSeen = blocksSeen;
      this.totalTransactions = totalTransactions;
      this.totalGasUsed = totalGasUsed;
      this.averageGasPerBlock = averageGasPerBlock;
    }
  }
}
