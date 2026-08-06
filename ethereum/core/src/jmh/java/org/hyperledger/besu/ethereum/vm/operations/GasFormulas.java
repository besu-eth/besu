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

import org.hyperledger.besu.evm.gascalculator.OsakaGasCalculator;

import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Function;

import org.openjdk.jmh.infra.BenchmarkParams;

public final class GasFormulas {

  private GasFormulas() {}

  private static final Map<Class<?>, Function<BenchmarkParams, OptionalLong>> REGISTRY =
      Map.of(
          CallDataCopyOperationBenchmark.class, CallDataCopyOperationBenchmark::gas,
          Keccak256Benchmark.class, Keccak256Benchmark::gas,
          SHA256Benchmark.class, SHA256Benchmark::gas,
          AddOperationBenchmark.class,
              p -> OptionalLong.of(new OsakaGasCalculator().getVeryLowTierGasCost()));

  public static OptionalLong compute(final BenchmarkParams params) {
    final String fqn = params.getBenchmark();
    final String className = fqn.substring(0, fqn.lastIndexOf('.'));
    try {
      final Class<?> clazz = Class.forName(className);
      final Function<BenchmarkParams, OptionalLong> formula = REGISTRY.get(clazz);
      return formula != null ? formula.apply(params) : OptionalLong.empty();
    } catch (ClassNotFoundException e) {
      return OptionalLong.empty();
    }
  }
}
