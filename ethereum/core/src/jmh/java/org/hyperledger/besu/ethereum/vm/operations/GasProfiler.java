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
package org.hyperledger.besu.ethereum.vm.operations;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

/**
 * JMH {@link InternalProfiler} that publishes the per-benchmark gas cost as a secondary metric
 * named {@code gas}. Runs outside the measurement window so it cannot affect the recorded ns/op
 * score.
 */
public class GasProfiler implements InternalProfiler {

  @Override
  public String getDescription() {
    return "Emits per-benchmark gas cost as a secondary metric named 'gas'";
  }

  @Override
  public void beforeIteration(
      final BenchmarkParams benchmarkParams, final IterationParams iterationParams) {}

  @Override
  public Collection<? extends Result<?>> afterIteration(
      final BenchmarkParams benchmarkParams,
      final IterationParams iterationParams,
      final IterationResult result) {
    final OptionalLong gas = GasFormulas.compute(benchmarkParams);
    if (gas.isEmpty()) {
      return Collections.emptyList();
    }
    return List.of(new ScalarResult("gas", (double) gas.getAsLong(), "gas", AggregationPolicy.AVG));
  }
}
