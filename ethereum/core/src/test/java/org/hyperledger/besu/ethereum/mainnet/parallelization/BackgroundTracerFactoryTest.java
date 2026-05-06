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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.SlowBlockTracer;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.evm.tracing.EVMExecutionMetricsTracer;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.plugin.services.tracer.BlockAwareOperationTracer;

import org.junit.jupiter.api.Test;

/** Unit tests for {@link BackgroundTracerFactory}. */
class BackgroundTracerFactoryTest {

  private static SlowBlockTracer newSlowBlockTracer() {
    return new SlowBlockTracer(0, mock(BlockAwareOperationTracer.class));
  }

  @Test
  void createBackgroundTracer_nullContext_returnsNoTracing() {
    assertThat(BackgroundTracerFactory.createBackgroundTracer(null))
        .isSameAs(OperationTracer.NO_TRACING);
  }

  @Test
  void createBackgroundTracer_nullTracer_returnsNoTracing() {
    final BlockProcessingContext bpc = mock(BlockProcessingContext.class);
    when(bpc.getOperationTracer()).thenReturn(null);
    assertThat(BackgroundTracerFactory.createBackgroundTracer(bpc))
        .isSameAs(OperationTracer.NO_TRACING);
  }

  @Test
  void createBackgroundTracer_withSlowBlockTracer_returnsNewEVMExecutionMetricsTracer() {
    final SlowBlockTracer slowBlockTracer = newSlowBlockTracer();
    final BlockProcessingContext bpc = mock(BlockProcessingContext.class);
    when(bpc.getOperationTracer()).thenReturn(slowBlockTracer);

    final OperationTracer background = BackgroundTracerFactory.createBackgroundTracer(bpc);

    assertThat(background).isInstanceOf(EVMExecutionMetricsTracer.class);
    assertThat(background).isNotSameAs(slowBlockTracer);
  }

  @Test
  void createBackgroundTracer_withUnknownTracer_returnsOriginal() {
    final OperationTracer unknownTracer = mock(OperationTracer.class);
    final BlockProcessingContext bpc = mock(BlockProcessingContext.class);
    when(bpc.getOperationTracer()).thenReturn(unknownTracer);

    final OperationTracer background = BackgroundTracerFactory.createBackgroundTracer(bpc);

    assertThat(background).isSameAs(unknownTracer);
  }

  @Test
  void hasMetricsTracer_slowBlockTracer() {
    assertThat(BackgroundTracerFactory.hasMetricsTracer(newSlowBlockTracer())).isTrue();
  }

  @Test
  void hasMetricsTracer_noMetricsTracer() {
    assertThat(BackgroundTracerFactory.hasMetricsTracer(mock(OperationTracer.class))).isFalse();
  }

  @Test
  void findSlowBlockTracer_directInstance() {
    final SlowBlockTracer sbt = newSlowBlockTracer();
    assertThat(BackgroundTracerFactory.findSlowBlockTracer(sbt)).contains(sbt);
  }

  @Test
  void findSlowBlockTracer_notPresent() {
    assertThat(BackgroundTracerFactory.findSlowBlockTracer(mock(OperationTracer.class))).isEmpty();
  }

  @Test
  void consolidateTracerResults_mergesMetricsAndIncrementsTxCount() {
    final SlowBlockTracer slowBlockTracer = newSlowBlockTracer();
    final BlockHeader blockHeader = mock(BlockHeader.class);
    slowBlockTracer.traceStartBlock(null, blockHeader, null, Address.ZERO);

    final BlockProcessingContext bpc = mock(BlockProcessingContext.class);
    when(bpc.getOperationTracer()).thenReturn(slowBlockTracer);

    final EVMExecutionMetricsTracer backgroundTracer = new EVMExecutionMetricsTracer();

    BackgroundTracerFactory.consolidateTracerResults(backgroundTracer, bpc);

    assertThat(slowBlockTracer.getExecutionStats().getTransactionCount())
        .as("tx_count should be incremented after consolidation")
        .isEqualTo(1);
  }

  @Test
  void consolidateTracerResults_nullContext_doesNotThrow() {
    final EVMExecutionMetricsTracer backgroundTracer = new EVMExecutionMetricsTracer();
    // Should not throw
    BackgroundTracerFactory.consolidateTracerResults(backgroundTracer, null);
  }
}
