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

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import java.util.Map;
import java.util.OptionalLong;
import java.util.function.BiFunction;

import org.apache.tuweni.bytes.Bytes;
import org.openjdk.jmh.infra.BenchmarkParams;

public final class GasFormulas {

  private GasFormulas() {}

  private static final Map<Class<?>, BiFunction<BenchmarkParams, GasCalculator, OptionalLong>>
      REGISTRY =
          Map.of(
              CallDataCopyOperationBenchmark.class, GasFormulas::callDataCopyGas,
              Keccak256Benchmark.class, GasFormulas::keccak256Gas,
              SHA256Benchmark.class, GasFormulas::sha256Gas,
              AddOperationBenchmark.class,
                  (p, calc) -> OptionalLong.of(calc.getVeryLowTierGasCost()));

  public static OptionalLong compute(final BenchmarkParams params, final GasCalculator calc) {
    final String fqn = params.getBenchmark();
    if (fqn.endsWith(".baseline")) {
      return OptionalLong.empty();
    }
    final String className = fqn.substring(0, fqn.lastIndexOf('.'));
    try {
      final Class<?> clazz = Class.forName(className);
      final BiFunction<BenchmarkParams, GasCalculator, OptionalLong> formula = REGISTRY.get(clazz);
      return formula != null ? formula.apply(params, calc) : OptionalLong.empty();
    } catch (ClassNotFoundException e) {
      return OptionalLong.empty();
    }
  }

  private static OptionalLong callDataCopyGas(
      final BenchmarkParams params, final GasCalculator calc) {
    final long dataSize = Long.parseLong(params.getParam("dataSize"));
    final MessageFrame frame = BenchmarkHelper.createMessageCallFrame();
    frame.expandMemory(0, dataSize);
    return OptionalLong.of(calc.dataCopyOperationGasCost(frame, 0, dataSize));
  }

  private static OptionalLong keccak256Gas(
      final BenchmarkParams params, final GasCalculator calc) {
    final long inputSize = Long.parseLong(params.getParam("inputSize"));
    final MessageFrame frame = BenchmarkHelper.createMessageCallFrame();
    frame.expandMemory(0, inputSize);
    return OptionalLong.of(calc.keccak256OperationGasCost(frame, 0, inputSize));
  }

  private static OptionalLong sha256Gas(final BenchmarkParams params, final GasCalculator calc) {
    final int inputSize = Integer.parseInt(params.getParam("inputSize"));
    return OptionalLong.of(calc.sha256PrecompiledContractGasCost(Bytes.wrap(new byte[inputSize])));
  }
}
