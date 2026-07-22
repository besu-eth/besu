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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConstructorArgumentsBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.ConstructorArguments;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
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
    final ConstructorArgumentsBuilder constructorArgumentsBuilder =
        new ConstructorArgumentsBuilder();
    constructorArgumentsBuilder
        .protocolSchedule(protocolSchedule)
        .protocolContext(protocolContext)
        .vertx(consensusEngineServer)
        .transactionPool(transactionPool)
        .metricsSystem(metricsSystem)
        .blockResultFactory(blockResultFactory)
        .ethPeers(ethPeers)
        .engineCallListener(engineQosTimer)
        .maxRequestBlocks(GET_PAYLOAD_BODIES_MAX_REQUEST_SIZE);

    if (mergeCoordinator.isPresent()) {
      final ConstructorArguments constructorArguments =
          constructorArgumentsBuilder.mergeCoordinator(mergeCoordinator.get()).build();
      final List<JsonRpcMethod> executionEngineApisSupported = new ArrayList<>();
      executionEngineApisSupported.addAll(
          createEngineForkchoiceUpdatedMethods(constructorArguments));

      executionEngineApisSupported.addAll(createEngineNewPayloadMethods(constructorArguments));

      executionEngineApisSupported.addAll(createEngineGetPayloadMethods(constructorArguments));

      executionEngineApisSupported.addAll(
          createEngineExchangeTransitionConfigurationMethods(constructorArguments));

      executionEngineApisSupported.addAll(
          createGetPayloadBodiesByHashMethods(constructorArguments));

      executionEngineApisSupported.addAll(
          createGetPayloadBodiesByRangeMethods(constructorArguments));

      executionEngineApisSupported.addAll(createGetBlobsMethods(constructorArguments));

      executionEngineApisSupported.addAll(
          Arrays.asList(
              new EngineExchangeCapabilities(constructorArguments),
              new EnginePreparePayloadDebug(constructorArguments),
              new EngineGetClientVersionV1(constructorArguments, clientVersion, commit)));

      return mapOf(executionEngineApisSupported);
    } else {
      return mapOf(
          List.copyOf(
              createEngineExchangeTransitionConfigurationMethods(
                  constructorArgumentsBuilder.build())));
    }
  }

  private Collection<? extends JsonRpcMethod> createEngineForkchoiceUpdatedMethods(
      final ConstructorArguments constructorArguments) {

    // special case at the first hardfork (Shanghai), before it was possible to call either V1 or V2
    // so both versions are scheduled at the beginning, and only V1 must be stopped at Shanghai
    // timestamp
    return VersionScheduler.startsFromBeginningUntil(EngineForkchoiceUpdatedV1::new, SHANGHAI)
        .thenAlsoFromBeginning(EngineForkchoiceUpdatedV2::new)
        .thenFrom(CANCUN, EngineForkchoiceUpdatedV3::new)
        .thenFrom(AMSTERDAM, EngineForkchoiceUpdatedV4::new)
        .build(constructorArguments);
  }

  private Collection<? extends JsonRpcMethod> createEngineNewPayloadMethods(
      final ConstructorArguments constructorArguments) {

    return VersionScheduler.startsFromBeginningUntil(EngineNewPayloadV1::new, SHANGHAI)
        .thenAlsoFromBeginning(EngineNewPayloadV2::new)
        .thenFrom(CANCUN, EngineNewPayloadV3::new)
        .thenFrom(PRAGUE, EngineNewPayloadV4::new)
        .thenFrom(AMSTERDAM, EngineNewPayloadV5::new)
        .build(constructorArguments);
  }

  private Collection<? extends JsonRpcMethod> createEngineGetPayloadMethods(
      final ConstructorArguments constructorArguments) {

    return VersionScheduler.startsFromBeginningUntil(EngineGetPayloadV1::new, SHANGHAI)
        .thenAlsoFromBeginning(EngineGetPayloadV2::new)
        .thenFrom(CANCUN, EngineGetPayloadV3::new)
        .thenFrom(PRAGUE, EngineGetPayloadV4::new)
        .thenFrom(OSAKA, EngineGetPayloadV5::new)
        .thenFrom(AMSTERDAM, EngineGetPayloadV6::new)
        .build(constructorArguments);
  }

  private Collection<? extends JsonRpcMethod> createEngineExchangeTransitionConfigurationMethods(
      final ConstructorArguments constructorArguments) {

    return VersionScheduler.startsFromBeginningUntil(
            EngineExchangeTransitionConfigurationV1::new, CANCUN)
        .build(constructorArguments);
  }

  @SuppressWarnings("unchecked")
  private Collection<? extends JsonRpcMethod> createGetPayloadBodiesByHashMethods(
      final ConstructorArguments constructorArguments) {
    return VersionScheduler.alwaysActive(
            EngineGetPayloadBodiesByHashV1::new, EngineGetPayloadBodiesByHashV2::new)
        .build(constructorArguments);
  }

  @SuppressWarnings("unchecked")
  private Collection<? extends JsonRpcMethod> createGetPayloadBodiesByRangeMethods(
      final ConstructorArguments constructorArguments) {
    return VersionScheduler.alwaysActive(
            EngineGetPayloadBodiesByRangeV1::new, EngineGetPayloadBodiesByRangeV2::new)
        .build(constructorArguments);
  }

  @SuppressWarnings("unchecked")
  private Collection<? extends JsonRpcMethod> createGetBlobsMethods(
      final ConstructorArguments constructorArguments) {

    return VersionScheduler.startsFrom(CANCUN, EngineGetBlobsV1::new)
        .thenFrom(OSAKA, EngineGetBlobsV2::new, EngineGetBlobsV3::new)
        .build(constructorArguments);
  }

  @VisibleForTesting
  static class VersionScheduler {
    final List<MethodVersionBuildData> readyMethods = new ArrayList<>();
    List<MethodVersionBuildData> pendingMethods = new ArrayList<>();

    /**
     * Creates one version of an engine method. Since all versioned engine methods share the same
     * constructor signature, their constructor references can be used directly, keeping method
     * instantiation free of reflection.
     */
    @FunctionalInterface
    interface EngineMethodFactory {
      ExecutionEngineJsonRpcMethod create(
          ConstructorArguments constructorArguments, HardforkId minFork, HardforkId maxFork);
    }

    static VersionScheduler startsFromBeginningUntil(
        final EngineMethodFactory firstVersion, final HardforkId to) {
      final VersionScheduler vs = new VersionScheduler();
      vs.readyMethods.add(new MethodVersionBuildData(firstVersion, false, null, to));
      return vs;
    }

    static VersionScheduler startsFrom(
        final HardforkId from, final EngineMethodFactory firstVersion) {
      final VersionScheduler vs = new VersionScheduler();
      vs.pendingMethods.add(new MethodVersionBuildData(firstVersion, false, from, null));
      return vs;
    }

    static VersionScheduler alwaysActive(final EngineMethodFactory... methods) {
      final VersionScheduler vs = new VersionScheduler();
      Arrays.stream(methods)
          .forEach(mvbd -> vs.readyMethods.add(MethodVersionBuildData.alwaysActive(mvbd)));
      return vs;
    }

    public VersionScheduler thenAlsoFromBeginning(final EngineMethodFactory method) {
      checkState(
          pendingMethods.isEmpty() || pendingMethods.stream().allMatch(mvbd -> mvbd.to == null),
          "This method can only be called for methods that are active since Paris hardfork");
      pendingMethods.add(new MethodVersionBuildData(method, false, null, null));
      return this;
    }

    final VersionScheduler thenFrom(
        final HardforkId hardforkId, final EngineMethodFactory... methods) {
      pendingMethods.forEach(mvbd -> readyMethods.add(mvbd.withTo(hardforkId)));
      pendingMethods = new ArrayList<>();
      Arrays.stream(methods)
          .forEach(
              method ->
                  pendingMethods.add(new MethodVersionBuildData(method, false, hardforkId, null)));
      return this;
    }

    List<? extends ExecutionEngineJsonRpcMethod> build(
        final ConstructorArguments constructorArguments) {
      readyMethods.addAll(pendingMethods);

      return readyMethods.stream()
          .filter(
              mv ->
                  mv.from == null
                      || constructorArguments.protocolSchedule().milestoneFor(mv.from).isPresent())
          .map(mv -> mv.factory.create(constructorArguments, mv.from, mv.to))
          .toList();
    }

    record MethodVersionBuildData(
        EngineMethodFactory factory, boolean alwaysActive, HardforkId from, HardforkId to) {

      MethodVersionBuildData withTo(final HardforkId hardforkId) {
        return new MethodVersionBuildData(factory, false, from, hardforkId);
      }

      static MethodVersionBuildData alwaysActive(final EngineMethodFactory factory) {
        return new MethodVersionBuildData(factory, true, null, null);
      }
    }
  }
}
