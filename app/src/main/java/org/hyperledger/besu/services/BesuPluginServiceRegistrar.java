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
package org.hyperledger.besu.services;

import org.hyperledger.besu.Runner;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.plugin.services.BesuEvents;
import org.hyperledger.besu.plugin.services.BlockSimulationService;
import org.hyperledger.besu.plugin.services.BlockchainService;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.PermissioningService;
import org.hyperledger.besu.plugin.services.RpcEndpointService;
import org.hyperledger.besu.plugin.services.SecurityModuleService;
import org.hyperledger.besu.plugin.services.StorageService;
import org.hyperledger.besu.plugin.services.TraceService;
import org.hyperledger.besu.plugin.services.TransactionPoolValidatorService;
import org.hyperledger.besu.plugin.services.TransactionSelectionService;
import org.hyperledger.besu.plugin.services.TransactionSimulationService;
import org.hyperledger.besu.plugin.services.TransactionValidatorService;
import org.hyperledger.besu.plugin.services.WorldStateService;
import org.hyperledger.besu.plugin.services.metrics.MetricCategoryRegistry;
import org.hyperledger.besu.plugin.services.mining.MiningService;
import org.hyperledger.besu.plugin.services.p2p.P2PService;
import org.hyperledger.besu.plugin.services.rlp.RlpConverterService;
import org.hyperledger.besu.plugin.services.sync.SynchronizationService;
import org.hyperledger.besu.plugin.services.transactionpool.TransactionPoolService;

/**
 * Single source of truth for registering plugin services with a {@link BesuPluginContextImpl}.
 *
 * <p>Both {@code BesuCommand} (production path) and {@code ThreadBesuNodeRunner} (acceptance-test
 * path) delegate to this class so that any future service addition only needs to be made in one
 * place.
 *
 * <p>Two phases match the lifecycle:
 *
 * <ol>
 *   <li>{@link #registerEarlyServices} – services available before {@link BesuController} is built;
 *       called from {@code preparePlugins} / {@code loadPluginContext}.
 *   <li>{@link #registerRuntimeServices} – services that wrap live objects only available after the
 *       controller and runner are built; called from {@code startPlugins} / {@code
 *       loadAdditionalServices}. Does <em>not</em> call {@code startPlugins()} — callers do that.
 * </ol>
 */
public final class BesuPluginServiceRegistrar {

  private BesuPluginServiceRegistrar() {}

  /**
   * Registers the early plugin services (phase 1 – before {@link BesuController} is built).
   *
   * <p>Intentionally omitted from this method:
   *
   * <ul>
   *   <li>{@code PicoCLIOptions} – wraps a CLI-specific {@code CommandLine}; not meaningful in
   *       non-CLI paths.
   *   <li>{@code BesuConfiguration} – constructed differently per path and registered by each
   *       caller.
   * </ul>
   *
   * @param pluginContext the plugin context to register services into
   * @param securityModuleService the security module service
   * @param storageService the storage service
   * @param metricCategoryRegistry the metric category registry
   * @param permissioningService the permissioning service
   * @param rpcEndpointService the RPC endpoint service
   * @param transactionSelectionService the transaction selection service
   * @param transactionPoolValidatorService the transaction pool validator service
   * @param transactionSimulationService the transaction simulation service
   * @param blockchainService the blockchain service
   * @param transactionValidatorService the transaction validator service
   */
  public static void registerEarlyServices(
      final BesuPluginContextImpl pluginContext,
      final SecurityModuleService securityModuleService,
      final StorageService storageService,
      final MetricCategoryRegistry metricCategoryRegistry,
      final PermissioningService permissioningService,
      final RpcEndpointService rpcEndpointService,
      final TransactionSelectionService transactionSelectionService,
      final TransactionPoolValidatorService transactionPoolValidatorService,
      final TransactionSimulationService transactionSimulationService,
      final BlockchainService blockchainService,
      final TransactionValidatorService transactionValidatorService) {

    pluginContext.addService(SecurityModuleService.class, securityModuleService);
    pluginContext.addService(StorageService.class, storageService);
    pluginContext.addService(MetricCategoryRegistry.class, metricCategoryRegistry);
    pluginContext.addService(PermissioningService.class, permissioningService);
    pluginContext.addService(RpcEndpointService.class, rpcEndpointService);
    pluginContext.addService(TransactionSelectionService.class, transactionSelectionService);
    pluginContext.addService(
        TransactionPoolValidatorService.class, transactionPoolValidatorService);
    pluginContext.addService(TransactionSimulationService.class, transactionSimulationService);
    pluginContext.addService(BlockchainService.class, blockchainService);
    pluginContext.addService(TransactionValidatorService.class, transactionValidatorService);
  }

  /**
   * Registers the runtime plugin services (phase 2 – after {@link BesuController} and {@link
   * Runner} are built).
   *
   * <p>Also calls {@link BesuController#getAdditionalPluginServices()
   * appendPluginServices(pluginContext)} so consensus-layer plugin services are included.
   *
   * <p>Does <em>not</em> call {@code pluginContext.startPlugins()} – callers are responsible for
   * that after any additional post-registration initialisation they need.
   *
   * <p><strong>Why {@link MetricsSystem} is in phase 2, not phase 1:</strong>
   *
   * <p>There are two independent constraints that together force {@code MetricsSystem} into the
   * runtime phase when running via {@code BesuCommand}:
   *
   * <ol>
   *   <li><em>Parsed configuration.</em> {@code MetricsSystem} is created from {@code
   *       MetricsConfiguration}, which is built from PicoCLI-parsed CLI flags (e.g. {@code
   *       --metrics-enabled}, {@code --metrics-category}). {@code preparePlugins()} (phase 1) runs
   *       before PicoCLI has parsed those flags, so the configuration is not yet available there.
   *   <li><em>Plugin-registered categories.</em> {@code MetricsConfiguration.validate()} rejects
   *       any {@code --metrics-category} values that are not present in the {@link
   *       MetricCategoryRegistry}. Plugins register their custom categories during {@code
   *       registerPlugins()}, which also runs in phase 1. Resolving the {@code MetricsSystem} (and
   *       thus calling {@code validate()}) before {@code registerPlugins()} has completed would
   *       therefore throw a {@code ParameterException} for any plugin-defined category passed on
   *       the command line (e.g. {@code --metrics-category TEST_METRIC_CATEGORY}).
   * </ol>
   *
   * <p>Both constraints are absent in the {@code ThreadBesuNodeRunner} path used by acceptance
   * tests: {@code MetricsConfiguration} is constructed directly in the test (no CLI parsing) and
   * there is no {@code validate()} call. The runtime-phase placement is nevertheless kept for
   * consistency between the two paths.
   *
   * @param pluginContext the plugin context to register services into
   * @param besuController the fully built Besu controller
   * @param runner the fully built runner (provides P2P network and in-process RPC)
   * @param metricsSystem the fully configured metrics system
   * @param miningConfiguration the active mining configuration
   */
  public static void registerRuntimeServices(
      final BesuPluginContextImpl pluginContext,
      final BesuController besuController,
      final Runner runner,
      final MetricsSystem metricsSystem,
      final MiningConfiguration miningConfiguration) {

    pluginContext.addService(
        BesuEvents.class,
        new BesuEventsImpl(
            besuController.getProtocolContext().getBlockchain(),
            besuController.getProtocolManager().getBlockBroadcaster(),
            besuController.getTransactionPool(),
            besuController.getSyncState(),
            besuController.getProtocolContext().getBadBlockManager()));

    pluginContext.addService(MetricsSystem.class, metricsSystem);

    pluginContext.addService(
        WorldStateService.class,
        new WorldStateServiceImpl(
            besuController.getProtocolContext().getWorldStateArchive(),
            besuController.getProtocolContext().getBlockchain()));

    pluginContext.addService(
        SynchronizationService.class,
        new SynchronizationServiceImpl(
            besuController.getSynchronizer(),
            besuController.getProtocolContext(),
            besuController.getProtocolSchedule(),
            besuController.getSyncState(),
            besuController.getProtocolContext().getWorldStateArchive()));

    pluginContext.addService(
        P2PService.class, new P2PServiceImpl(runner.getP2PNetwork(), besuController.getEthPeers()));

    pluginContext.addService(
        TransactionPoolService.class,
        new TransactionPoolServiceImpl(besuController.getTransactionPool()));

    pluginContext.addService(
        RlpConverterService.class,
        new RlpConverterServiceImpl(besuController.getProtocolSchedule()));

    pluginContext.addService(
        TraceService.class,
        new TraceServiceImpl(
            new BlockchainQueries(
                besuController.getProtocolSchedule(),
                besuController.getProtocolContext().getBlockchain(),
                besuController.getProtocolContext().getWorldStateArchive(),
                miningConfiguration),
            besuController.getProtocolSchedule()));

    pluginContext.addService(
        MiningService.class, new MiningServiceImpl(besuController.getMiningCoordinator()));

    pluginContext.addService(
        BlockSimulationService.class,
        new BlockSimulatorServiceImpl(
            besuController.getProtocolContext().getWorldStateArchive(),
            miningConfiguration,
            besuController.getTransactionSimulator(),
            besuController.getProtocolSchedule(),
            besuController.getProtocolContext().getBlockchain()));

    besuController.getAdditionalPluginServices().appendPluginServices(pluginContext);
  }
}
