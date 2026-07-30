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
package org.hyperledger.besu.ethereum.api.jsonrpc.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.SHANGHAI;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConstructorArgumentsBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.ConstructorArguments;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineCallListener;
import org.hyperledger.besu.ethereum.api.jsonrpc.methods.ExecutionEngineJsonRpcMethods.VersionScheduler;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;

class VersionSchedulerTest {

  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
  private final ConstructorArguments constructorArguments =
      new ConstructorArgumentsBuilder()
          .protocolSchedule(protocolSchedule)
          .protocolContext(mock(ProtocolContext.class))
          .vertx(mock(Vertx.class))
          .engineCallListener(mock(EngineCallListener.class))
          .mergeCoordinator(mock(MergeMiningCoordinator.class))
          .ethPeers(mock(EthPeers.class))
          .metricsSystem(new NoOpMetricsSystem())
          .build();

  private final RecordingFactory v1 = new RecordingFactory();
  private final RecordingFactory v2 = new RecordingFactory();
  private final RecordingFactory v3 = new RecordingFactory();
  private final RecordingFactory v4 = new RecordingFactory();

  @Test
  void buildsEveryVersionWithItsForkWindowWhenAllMilestonesAreScheduled() {
    when(protocolSchedule.milestoneFor(any())).thenReturn(Optional.of(0L));

    final var builtMethods = schedule();

    assertThat(builtMethods).containsExactly(v1.instance, v2.instance, v3.instance, v4.instance);
    v1.assertForkWindow(null, SHANGHAI);
    v2.assertForkWindow(null, CANCUN);
    v3.assertForkWindow(CANCUN, AMSTERDAM);
    v4.assertForkWindow(AMSTERDAM, null);
  }

  @Test
  void skipsVersionsStartingAtUnscheduledForks() {
    when(protocolSchedule.milestoneFor(CANCUN)).thenReturn(Optional.of(0L));
    when(protocolSchedule.milestoneFor(AMSTERDAM)).thenReturn(Optional.empty());

    final var builtMethods = schedule();

    assertThat(builtMethods).containsExactly(v1.instance, v2.instance, v3.instance);
    assertThat(v4.invocations).isZero();
  }

  @Test
  void alwaysBuildsVersionsActiveFromTheBeginning() {
    when(protocolSchedule.milestoneFor(any())).thenReturn(Optional.empty());

    final var builtMethods = schedule();

    assertThat(builtMethods).containsExactly(v1.instance, v2.instance);
    assertThat(v3.invocations).isZero();
    assertThat(v4.invocations).isZero();
  }

  @Test
  void passesBuildArgumentsToEveryFactory() {
    when(protocolSchedule.milestoneFor(any())).thenReturn(Optional.of(0L));

    schedule();

    for (final RecordingFactory factory : List.of(v1, v2, v3, v4)) {
      assertThat(factory.invocations).isOne();
      assertThat(factory.constructorArguments).isSameAs(constructorArguments);
    }
  }

  @Test
  void schedulesEveryMethodOfTheSameForkOnTheSameWindow() {
    when(protocolSchedule.milestoneFor(any())).thenReturn(Optional.of(0L));
    final RecordingFactory alsoFromCancun = new RecordingFactory();

    final List<ExecutionEngineJsonRpcMethod> builtMethods =
        List.copyOf(
            VersionScheduler.startsFromBeginningUntil(v1, SHANGHAI)
                .thenFrom(CANCUN, v3, alsoFromCancun)
                .thenFrom(AMSTERDAM, v4)
                .build(constructorArguments));

    assertThat(builtMethods)
        .containsExactly(v1.instance, v3.instance, alsoFromCancun.instance, v4.instance);
    v3.assertForkWindow(CANCUN, AMSTERDAM);
    alsoFromCancun.assertForkWindow(CANCUN, AMSTERDAM);
  }

  private List<ExecutionEngineJsonRpcMethod> schedule() {
    return List.copyOf(
        VersionScheduler.startsFromBeginningUntil(v1, SHANGHAI)
            .thenAlsoFromBeginning(v2)
            .thenFrom(CANCUN, v3)
            .thenFrom(AMSTERDAM, v4)
            .build(constructorArguments));
  }

  private static final class RecordingFactory implements VersionScheduler.EngineMethodFactory {
    final ExecutionEngineJsonRpcMethod instance = mock(ExecutionEngineJsonRpcMethod.class);
    int invocations;
    ConstructorArguments constructorArguments;
    HardforkId minFork;
    HardforkId maxFork;

    @Override
    public ExecutionEngineJsonRpcMethod create(
        final ConstructorArguments constructorArguments,
        final HardforkId minFork,
        final HardforkId maxFork) {
      invocations++;
      this.constructorArguments = constructorArguments;
      this.minFork = minFork;
      this.maxFork = maxFork;
      return instance;
    }

    void assertForkWindow(final HardforkId expectedMinFork, final HardforkId expectedMaxFork) {
      assertThat(minFork).isEqualTo(expectedMinFork);
      assertThat(maxFork).isEqualTo(expectedMaxFork);
    }
  }
}
