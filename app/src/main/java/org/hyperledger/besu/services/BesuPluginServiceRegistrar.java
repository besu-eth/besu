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
import org.hyperledger.besu.ethereum.api.pluginadapter.HealthCheckServiceImpl;
import org.hyperledger.besu.ethereum.api.pluginadapter.InProcessRpcServiceImpl;
import org.hyperledger.besu.ethereum.api.pluginadapter.RpcEndpointRegistryImpl;
import org.hyperledger.besu.ethereum.api.pluginadapter.TraceServiceImpl;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.blockcreation.pluginadapter.MiningServiceImpl;
import org.hyperledger.besu.ethereum.chain.pluginadapter.BlockchainServiceImpl;
import org.hyperledger.besu.ethereum.chain.pluginadapter.RlpConverterServiceImpl;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.pluginadapter.TransactionPoolServiceImpl;
import org.hyperledger.besu.ethereum.transaction.pluginadapter.BlockSimulatorServiceImpl;
import org.hyperledger.besu.ethereum.transaction.pluginadapter.TransactionSimulationServiceImpl;
import org.hyperledger.besu.ethereum.worldstate.pluginadapter.WorldStateServiceImpl;
import org.hyperledger.besu.plugin.services.BesuEvents;
import org.hyperledger.besu.plugin.services.BlockSimulationService;
import org.hyperledger.besu.plugin.services.BlockchainService;
import org.hyperledger.besu.plugin.services.HealthCheckService;
import org.hyperledger.besu.plugin.services.InProcessRpcService;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.PermissioningService;
import org.hyperledger.besu.plugin.services.RpcEndpointRegistry;
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
import org.hyperledger.besu.plugin.services.p2p.PeerEventService;
import org.hyperledger.besu.plugin.services.rlp.RlpConverterService;
import org.hyperledger.besu.plugin.services.sync.SyncEventService;
import org.hyperledger.besu.plugin.services.sync.SynchronizationService;
import org.hyperledger.besu.plugin.services.transactionpool.TransactionPoolService;

/**
 * Single source of truth for adding Besu-provided services to a {@link BesuPluginContextImpl}.
 *
 * <p>Both {@code BesuCommand} (production path) and {@code ThreadBesuNodeRunner} (acceptance-test
 * path) delegate to this class so that any future service addition only needs to be made in one
 * place.
 *
 * <p>A service is added at the boundary of the phase whose tier marker it carries, when its
 * dependencies exist:
 *
 * <ol>
 *   <li>{@link #registerRegistrationServices} – the {@code RegistrationService} types, before any
 *       plugin registers. The configuration views are constructed differently per path and added by
 *       each caller.
 *   <li>{@link #registerStartServices} – the {@code StartService} types, after the controller and
 *       runner are built and before {@code startPlugins()}. Each adapter is constructed here with
 *       its dependencies, so a plugin never sees a service that exists but is not usable.
 *   <li>{@link #registerRunningServices} – the {@code RunningService} types, after the main loop
 *       has started and before {@code afterMainLoop()}.
 * </ol>
 */
public final class BesuPluginServiceRegistrar {

  private BesuPluginServiceRegistrar() {}

  /**
   * Adds the registration-tier services, before any plugin registers.
   *
   * @param pluginContext the plugin context to add services to
   * @param securityModuleService the security module service
   * @param storageService the storage service
   * @param metricCategoryRegistry the metric category registry
   * @param permissioningService the permissioning service
   * @param rpcEndpointRegistry the RPC endpoint registry
   * @param transactionSelectionService the transaction selection service
   * @param transactionPoolValidatorService the transaction pool validator service
   * @param transactionValidatorService the transaction validator service
   * @param healthCheckService the health check service
   */
  public static void registerRegistrationServices(
      final BesuPluginContextImpl pluginContext,
      final SecurityModuleService securityModuleService,
      final StorageService storageService,
      final MetricCategoryRegistry metricCategoryRegistry,
      final PermissioningService permissioningService,
      final RpcEndpointRegistryImpl rpcEndpointRegistry,
      final TransactionSelectionService transactionSelectionService,
      final TransactionPoolValidatorService transactionPoolValidatorService,
      final TransactionValidatorService transactionValidatorService,
      final HealthCheckServiceImpl healthCheckService) {

    pluginContext.addService(SecurityModuleService.class, securityModuleService);
    pluginContext.addService(StorageService.class, storageService);
    pluginContext.addService(MetricCategoryRegistry.class, metricCategoryRegistry);
    pluginContext.addService(PermissioningService.class, permissioningService);
    pluginContext.addService(RpcEndpointRegistry.class, rpcEndpointRegistry);
    pluginContext.addService(TransactionSelectionService.class, transactionSelectionService);
    pluginContext.addService(
        TransactionPoolValidatorService.class, transactionPoolValidatorService);
    pluginContext.addService(TransactionValidatorService.class, transactionValidatorService);
    pluginContext.addService(HealthCheckService.class, healthCheckService);
  }

  /**
   * Adds the start-tier services, after {@link BesuController} and {@link Runner} are built and
   * before {@code startPlugins()}.
   *
   * <p>Also calls {@link BesuController#getAdditionalPluginServices()
   * appendPluginServices(pluginContext)} so consensus-layer plugin services are included.
   *
   * <p>Does <em>not</em> call {@code pluginContext.startPlugins()} – callers are responsible for
   * that after any additional post-registration initialisation they need.
   *
   * @param pluginContext the plugin context to add services to
   * @param besuController the fully built Besu controller
   * @param runner the fully built runner (provides the in-process RPC methods and the network)
   * @param metricsSystem the fully configured metrics system
   * @param miningConfiguration the active mining configuration
   * @param inProcessRpcEnabled whether in-process RPC is enabled on this node
   */
  public static void registerStartServices(
      final BesuPluginContextImpl pluginContext,
      final BesuController besuController,
      final Runner runner,
      final MetricsSystem metricsSystem,
      final MiningConfiguration miningConfiguration,
      final boolean inProcessRpcEnabled) {

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
        BlockchainService.class,
        new BlockchainServiceImpl(
            besuController.getProtocolContext().getBlockchain(),
            besuController.getProtocolSchedule(),
            besuController.getProtocolManager().getBlockBroadcaster(),
            besuController.getProtocolContext().getBadBlockManager()));

    pluginContext.addService(
        TransactionSimulationService.class,
        new TransactionSimulationServiceImpl(
            besuController.getProtocolContext().getBlockchain(),
            besuController.getTransactionSimulator()));

    pluginContext.addService(
        InProcessRpcService.class,
        new InProcessRpcServiceImpl(inProcessRpcEnabled, runner.getInProcessRpcMethods()));

    pluginContext.addService(
        WorldStateService.class,
        new WorldStateServiceImpl(
            besuController.getProtocolContext().getWorldStateArchive(),
            besuController.getProtocolContext().getBlockchain()));

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

    // the subscription halves of sync and P2P: a subscription has to be in place before the
    // synchronizer and the network start producing events, which they do with the main loop
    pluginContext.addService(
        SyncEventService.class, new SyncEventServiceImpl(besuController.getSyncState()));
    pluginContext.addService(
        PeerEventService.class, new PeerEventServiceImpl(runner.getP2PNetwork()));

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

  /**
   * Adds the running-tier services, after the main loop has started and before {@code
   * afterMainLoop()}. Their components (the peer-to-peer network, the synchronizer, the mining
   * coordinator) only run from the main loop, so they are not offered earlier.
   *
   * @param pluginContext the plugin context to add services to
   * @param besuController the fully built Besu controller
   * @param runner the running runner (provides the P2P network)
   */
  public static void registerRunningServices(
      final BesuPluginContextImpl pluginContext,
      final BesuController besuController,
      final Runner runner) {

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

    registerMiningService(pluginContext, besuController);
  }

  @SuppressWarnings("removal") // MiningService is deprecated for removal; drop this with it
  private static void registerMiningService(
      final BesuPluginContextImpl pluginContext, final BesuController besuController) {
    pluginContext.addService(
        MiningService.class, new MiningServiceImpl(besuController.getMiningCoordinator()));
  }
}
