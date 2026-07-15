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

import java.util.OptionalLong;

import org.openjdk.jmh.infra.BenchmarkParams;

/** Per-benchmark gas-cost formulas keyed by the JMH benchmark-method FQN. */
public final class GasFormulas {

  private GasFormulas() {}

  /**
   * Compute the static gas cost of one benchmark invocation.
   *
   * @param params the JMH benchmark params carrying the FQN and all {@code @Param} values
   * @return an {@link OptionalLong} carrying the gas cost, or empty if the benchmark is not mapped
   */
  public static OptionalLong compute(final BenchmarkParams params) {
    final String benchmark = params.getBenchmark();

    if (benchmark.endsWith(".CallDataCopyOperationBenchmark.executeOperation")) {
      final int dataSize = Integer.parseInt(params.getParam("dataSize"));
      return OptionalLong.of(3L + 3L * ((dataSize + 31) / 32));
    }

    if (benchmark.endsWith(".Keccak256Benchmark.executeOperation")) {
      final int inputSize = Integer.parseInt(params.getParam("inputSize"));
      return OptionalLong.of(30L + 6L * ((inputSize + 31) / 32));
    }

    if (benchmark.endsWith(".SHA256Benchmark.sha256")) {
      final int inputSize = Integer.parseInt(params.getParam("inputSize"));
      return OptionalLong.of(60L + 12L * ((inputSize + 31) / 32));
    }

    return OptionalLong.empty();
  }
}
