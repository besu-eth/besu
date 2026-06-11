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
package org.hyperledger.besu.ethereum.api.jsonrpc.methods;

import static com.google.common.base.Preconditions.checkState;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.OSAKA;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PRAGUE;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.SHANGHAI;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineExchangeCapabilities;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineExchangeTransitionConfigurationV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineForkchoiceUpdatedV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineForkchoiceUpdatedV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineForkchoiceUpdatedV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineForkchoiceUpdatedV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineGetBlobsV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineGetBlobsV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineGetBlobsV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineGetClientVersionV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineGetPayloadBodiesByHashV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineGetPayloadBodiesByHashV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineGetPayloadBodiesByRangeV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineGetPayloadBodiesByRangeV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineGetPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineGetPayloadV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineGetPayloadV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineGetPayloadV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineGetPayloadV5;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineGetPayloadV6;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineNewPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineNewPayloadV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineNewPayloadV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineNewPayloadV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineNewPayloadV5;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EnginePreparePayloadDebug;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineQosTimer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResultFactory;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import io.vertx.core.Vertx;

public class ExecutionEngineJsonRpcMethods extends ApiGroupJsonRpcMethods {
  private static final int GET_PAYLOAD_BODIES_MAX_REQUEST_SIZE = 1024;
  private final BlockResultFactory blockResultFactory = new BlockResultFactory();

  private final Optional<MergeMiningCoordinator> mergeCoordinator;
  private final ProtocolSchedule protocolSchedule;
  private final ProtocolContext protocolContext;
  private final EthPeers ethPeers;
  private final Vertx consensusEngineServer;
  private final String clientVersion;
  private final String commit;
  private final TransactionPool transactionPool;
  private final MetricsSystem metricsSystem;

  ExecutionEngineJsonRpcMethods(
      final MiningCoordinator miningCoordinator,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EthPeers ethPeers,
      final Vertx consensusEngineServer,
      final String clientVersion,
      final String commit,
      final TransactionPool transactionPool,
      final MetricsSystem metricsSystem) {
    this.mergeCoordinator =
        Optional.ofNullable(miningCoordinator)
            .filter(MiningCoordinator::isCompatibleWithEngineApi)
            .map(MergeMiningCoordinator.class::cast);
    this.protocolSchedule = protocolSchedule;
    this.protocolContext = protocolContext;
    this.ethPeers = ethPeers;
    this.consensusEngineServer = consensusEngineServer;
    this.clientVersion = clientVersion;
    this.commit = commit;
    this.transactionPool = transactionPool;
    this.metricsSystem = metricsSystem;
  }

  @Override
  protected String getApiGroup() {
    return RpcApis.ENGINE.name();
  }

  @Override
  protected Map<String, JsonRpcMethod> create() {
    final EngineQosTimer engineQosTimer = new EngineQosTimer(consensusEngineServer);
    if (mergeCoordinator.isPresent()) {
      List<JsonRpcMethod> executionEngineApisSupported = new ArrayList<>();
      executionEngineApisSupported.addAll(
          createEngineForkchoiceUpdatedMethods(
              consensusEngineServer,
              protocolSchedule,
              protocolContext,
              mergeCoordinator.get(),
              engineQosTimer));

      executionEngineApisSupported.addAll(
          createEngineNewPayloadMethods(
              consensusEngineServer,
              protocolSchedule,
              protocolContext,
              mergeCoordinator.get(),
              ethPeers,
              engineQosTimer,
              metricsSystem));

      executionEngineApisSupported.addAll(
          createEngineGetPayloadMethods(
              consensusEngineServer,
              protocolSchedule,
              protocolContext,
              mergeCoordinator.get(),
              engineQosTimer));

      executionEngineApisSupported.addAll(
          createEngineExchangeTransitionConfigurationMethods(
              consensusEngineServer, protocolSchedule, protocolContext, engineQosTimer));

      executionEngineApisSupported.addAll(
          createGetPayloadBodiesByHashMethods(
              consensusEngineServer,
              protocolSchedule,
              protocolContext,
              engineQosTimer,
              GET_PAYLOAD_BODIES_MAX_REQUEST_SIZE));

      executionEngineApisSupported.addAll(
          createGetPayloadBodiesByRangeMethods(
              consensusEngineServer,
              protocolSchedule,
              protocolContext,
              engineQosTimer,
              GET_PAYLOAD_BODIES_MAX_REQUEST_SIZE));

      executionEngineApisSupported.addAll(
          createGetBlobsMethods(
              consensusEngineServer,
              protocolSchedule,
              protocolContext,
              engineQosTimer,
              transactionPool,
              metricsSystem));

      executionEngineApisSupported.addAll(
          Arrays.asList(
              new EngineExchangeCapabilities(
                  protocolSchedule, protocolContext, consensusEngineServer, engineQosTimer),
              new EnginePreparePayloadDebug(
                  protocolSchedule,
                  protocolContext,
                  consensusEngineServer,
                  engineQosTimer,
                  mergeCoordinator.get()),
              new EngineGetClientVersionV1(
                  protocolSchedule,
                  protocolContext,
                  consensusEngineServer,
                  engineQosTimer,
                  clientVersion,
                  commit)));

      return mapOf(executionEngineApisSupported);
    } else {
      return mapOf(
          List.copyOf(
              createEngineExchangeTransitionConfigurationMethods(
                  consensusEngineServer, protocolSchedule, protocolContext, engineQosTimer)));
    }
  }

  private Collection<? extends JsonRpcMethod> createEngineForkchoiceUpdatedMethods(
      final Vertx consensusEngineServer,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final MergeMiningCoordinator mergeMiningCoordinator,
      final EngineQosTimer engineQosTimer) {

    // special case at the first hardfork (Shanghai), before it was possible to call either V1 or V2
    // so both versions are scheduled at the beginning, and only V1 must be stopped at Shanghai
    // timestamp
    return VersionScheduler.startsFromBeginningUntil(EngineForkchoiceUpdatedV1.class, SHANGHAI)
        .thenAlsoFromBeginning(EngineForkchoiceUpdatedV2.class)
        .thenFrom(CANCUN, EngineForkchoiceUpdatedV3.class)
        .thenFrom(AMSTERDAM, EngineForkchoiceUpdatedV4.class)
        .build(
            protocolSchedule,
            protocolContext,
            consensusEngineServer,
            engineQosTimer,
            mergeMiningCoordinator);
  }

  private Collection<? extends JsonRpcMethod> createEngineNewPayloadMethods(
      final Vertx consensusEngineServer,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final MergeMiningCoordinator mergeMiningCoordinator,
      final EthPeers ethPeers,
      final EngineQosTimer engineQosTimer,
      final MetricsSystem metricsSystem) {

    return VersionScheduler.startsFromBeginningUntil(EngineNewPayloadV1.class, SHANGHAI)
        .thenAlsoFromBeginning(EngineNewPayloadV2.class)
        .thenFrom(CANCUN, EngineNewPayloadV3.class)
        .thenFrom(PRAGUE, EngineNewPayloadV4.class)
        .thenFrom(AMSTERDAM, EngineNewPayloadV5.class)
        .build(
            protocolSchedule,
            protocolContext,
            consensusEngineServer,
            engineQosTimer,
            mergeMiningCoordinator,
            ethPeers,
            metricsSystem);
  }

  private Collection<? extends JsonRpcMethod> createEngineGetPayloadMethods(
      final Vertx consensusEngineServer,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final MergeMiningCoordinator mergeMiningCoordinator,
      final EngineQosTimer engineQosTimer) {

    return VersionScheduler.startsFromBeginningUntil(EngineGetPayloadV1.class, SHANGHAI)
        .thenAlsoFromBeginning(EngineGetPayloadV2.class)
        .thenFrom(CANCUN, EngineGetPayloadV3.class)
        .thenFrom(PRAGUE, EngineGetPayloadV4.class)
        .thenFrom(OSAKA, EngineGetPayloadV5.class)
        .thenFrom(AMSTERDAM, EngineGetPayloadV6.class)
        .build(
            protocolSchedule,
            protocolContext,
            consensusEngineServer,
            engineQosTimer,
            mergeMiningCoordinator,
            blockResultFactory);
  }

  private Collection<? extends JsonRpcMethod> createEngineExchangeTransitionConfigurationMethods(
      final Vertx consensusEngineServer,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EngineQosTimer engineQosTimer) {

    return VersionScheduler.startsFromBeginningUntil(
            EngineExchangeTransitionConfigurationV1.class, CANCUN)
        .build(protocolSchedule, protocolContext, consensusEngineServer, engineQosTimer);
  }

  @SuppressWarnings("unchecked")
  private Collection<? extends JsonRpcMethod> createGetPayloadBodiesByHashMethods(
      final Vertx consensusEngineServer,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EngineQosTimer engineQosTimer,
      final int maxRequestSize) {
    return VersionScheduler.alwaysActive(
            EngineGetPayloadBodiesByHashV1.class, EngineGetPayloadBodiesByHashV2.class)
        .build(
            protocolSchedule,
            protocolContext,
            consensusEngineServer,
            engineQosTimer,
            maxRequestSize);
  }

  @SuppressWarnings("unchecked")
  private Collection<? extends JsonRpcMethod> createGetPayloadBodiesByRangeMethods(
      final Vertx consensusEngineServer,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EngineQosTimer engineQosTimer,
      final int maxRequestSize) {
    return VersionScheduler.alwaysActive(
            EngineGetPayloadBodiesByRangeV1.class, EngineGetPayloadBodiesByRangeV2.class)
        .build(
            protocolSchedule,
            protocolContext,
            consensusEngineServer,
            engineQosTimer,
            maxRequestSize);
  }

  @SuppressWarnings("unchecked")
  private Collection<? extends JsonRpcMethod> createGetBlobsMethods(
      final Vertx consensusEngineServer,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EngineQosTimer engineQosTimer,
      final TransactionPool transactionPool,
      final MetricsSystem metricsSystem) {

    return VersionScheduler.startsFrom(CANCUN, EngineGetBlobsV1.class)
        .thenFrom(OSAKA, EngineGetBlobsV2.class, EngineGetBlobsV3.class)
        .build(
            protocolSchedule,
            protocolContext,
            consensusEngineServer,
            engineQosTimer,
            transactionPool,
            metricsSystem);
  }

  private static class VersionScheduler {
    final List<MethodVersionBuildData> readyMethods = new ArrayList<>();
    List<MethodVersionBuildData> pendingMethods = new ArrayList<>();

    static VersionScheduler startsFromBeginningUntil(
        final Class<? extends ExecutionEngineJsonRpcMethod> firstVersion, final HardforkId to) {
      final VersionScheduler vs = new VersionScheduler();
      vs.readyMethods.add(new MethodVersionBuildData(firstVersion, false, null, to));
      return vs;
    }

    static VersionScheduler startsFrom(
        final HardforkId from, final Class<? extends ExecutionEngineJsonRpcMethod> firstVersion) {
      final VersionScheduler vs = new VersionScheduler();
      vs.pendingMethods.add(new MethodVersionBuildData(firstVersion, false, from, null));
      return vs;
    }

    @SafeVarargs
    static VersionScheduler alwaysActive(
        final Class<? extends ExecutionEngineJsonRpcMethod>... methods) {
      final VersionScheduler vs = new VersionScheduler();
      Arrays.stream(methods)
          .forEach(mvbd -> vs.readyMethods.add(MethodVersionBuildData.alwaysActive(mvbd)));
      return vs;
    }

    public VersionScheduler thenAlsoFromBeginning(
        final Class<? extends ExecutionEngineJsonRpcMethod> method) {
      checkState(
          pendingMethods.isEmpty() || pendingMethods.stream().allMatch(mvbd -> mvbd.to == null),
          "This method can only be called for methods that are active since Paris hardfork");
      pendingMethods.add(new MethodVersionBuildData(method, false, null, null));
      return this;
    }

    @SafeVarargs
    final VersionScheduler thenFrom(
        final HardforkId hardforkId,
        final Class<? extends ExecutionEngineJsonRpcMethod>... methods) {
      pendingMethods.forEach(mvbd -> readyMethods.add(mvbd.withTo(hardforkId)));
      pendingMethods = new ArrayList<>();
      Arrays.stream(methods)
          .forEach(
              method ->
                  pendingMethods.add(new MethodVersionBuildData(method, false, hardforkId, null)));
      return this;
    }

    List<? extends ExecutionEngineJsonRpcMethod> build(
        final ProtocolSchedule protocolSchedule, final Object... constructorArgs) {
      readyMethods.addAll(pendingMethods);

      return readyMethods.stream()
          .filter(mv -> mv.from == null || protocolSchedule.milestoneFor(mv.from).isPresent())
          .map(
              mv -> {
                try {
                  @SuppressWarnings("unchecked")
                  final var constructor =
                      (java.lang.reflect.Constructor<? extends ExecutionEngineJsonRpcMethod>)
                          mv.versionClass.getDeclaredConstructors()[0];
                  constructor.setAccessible(true);

                  final Object[] constructorArgsFull =
                      Stream.concat(
                              Stream.of(protocolSchedule),
                              Stream.concat(
                                  Arrays.stream(constructorArgs),
                                  mv.alwaysActive ? Stream.empty() : Stream.of(mv.from, mv.to)))
                          .toArray();

                  return constructor.newInstance(constructorArgsFull);
                } catch (InstantiationException
                    | IllegalAccessException
                    | InvocationTargetException e) {
                  throw new RuntimeException(e);
                }
              })
          .toList();
    }

    record MethodVersionBuildData(
        Class<? extends ExecutionEngineJsonRpcMethod> versionClass,
        boolean alwaysActive,
        HardforkId from,
        HardforkId to) {

      MethodVersionBuildData withTo(final HardforkId hardforkId) {
        return new MethodVersionBuildData(versionClass, false, from, hardforkId);
      }

      static MethodVersionBuildData alwaysActive(
          final Class<? extends ExecutionEngineJsonRpcMethod> versionClass) {
        return new MethodVersionBuildData(versionClass, true, null, null);
      }
    }
  }
}
