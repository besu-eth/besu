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
package org.hyperledger.besu.services.pipeline;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.metrics.noop.NoOpMetricsSystem.NO_OP_COUNTER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.hyperledger.besu.plugin.services.metrics.Counter;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

public class AggregatingReadPipeTest {

  private final Pipe<List<String>> source =
      new Pipe<>(10, NO_OP_COUNTER, NO_OP_COUNTER, NO_OP_COUNTER, "source_pipe");
  private final Counter aggregateCounter = mock(Counter.class);
  private final AggregatingReadPipe<String> aggregatingPipe =
      new AggregatingReadPipe<>(source, aggregateCounter);

  @Test
  public void shouldGetSingleBatchContainingAllInputBatches() {
    source.put(asList("a", "b"));
    source.put(asList("c"));
    source.put(asList("d", "e"));
    source.close();

    assertThat(aggregatingPipe.get()).containsExactly("a", "b", "c", "d", "e");
    assertThat(aggregatingPipe.get()).isNull();

    verify(aggregateCounter, times(1)).inc();
  }

  @Test
  public void shouldPollSingleBatchOnlyWhenInputIsComplete() {
    source.put(asList("a", "b"));

    assertThat(aggregatingPipe.poll()).isNull();
    verifyNoInteractions(aggregateCounter);

    source.put(asList("c"));
    source.close();

    assertThat(aggregatingPipe.poll()).containsExactly("a", "b", "c");
    assertThat(aggregatingPipe.poll()).isNull();

    verify(aggregateCounter, times(1)).inc();
  }

  @Test
  public void shouldDrainToRespectMaxElements() {
    source.put(asList("a"));
    source.put(asList("b", "c"));
    source.close();

    final List<List<String>> output = new ArrayList<>();

    assertThat(aggregatingPipe.drainTo(output, 0)).isZero();
    assertThat(output).isEmpty();

    assertThat(aggregatingPipe.drainTo(output, 1)).isEqualTo(1);
    assertThat(output).containsExactly(asList("a"));

    assertThat(aggregatingPipe.drainTo(output, 1)).isEqualTo(1);
    assertThat(output).containsExactly(asList("a"), asList("b"));

    assertThat(aggregatingPipe.drainTo(output, 1)).isEqualTo(1);
    assertThat(output).containsExactly(asList("a"), asList("b"), asList("c"));

    assertThat(aggregatingPipe.drainTo(output, 1)).isZero();

    verify(aggregateCounter, times(3)).inc();
  }

  @Test
  public void shouldDrainAvailableDataAndLeaveRemainingForPoll() {
    source.put(asList("a", "b"));

    final List<List<String>> output = new ArrayList<>();

    assertThat(aggregatingPipe.drainTo(output, 1)).isEqualTo(1);
    assertThat(output).containsExactly(asList("a"));

    source.close();

    assertThat(aggregatingPipe.poll()).containsExactly("b");
  }

  @Test
  public void shouldTrackHasMoreAcrossAggregationLifecycle() {
    assertThat(aggregatingPipe.hasMore()).isTrue();

    source.put(asList("a"));
    assertThat(aggregatingPipe.poll()).isNull();
    assertThat(aggregatingPipe.hasMore()).isTrue();

    source.close();
    assertThat(aggregatingPipe.poll()).containsExactly("a");
    assertThat(aggregatingPipe.hasMore()).isFalse();
  }
}
