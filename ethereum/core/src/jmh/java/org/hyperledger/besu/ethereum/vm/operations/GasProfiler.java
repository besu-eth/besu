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

import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.BerlinGasCalculator;
import org.hyperledger.besu.evm.gascalculator.ByzantiumGasCalculator;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.gascalculator.ConstantinopleGasCalculator;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.HomesteadGasCalculator;
import org.hyperledger.besu.evm.gascalculator.IstanbulGasCalculator;
import org.hyperledger.besu.evm.gascalculator.LondonGasCalculator;
import org.hyperledger.besu.evm.gascalculator.OsakaGasCalculator;
import org.hyperledger.besu.evm.gascalculator.PetersburgGasCalculator;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;
import org.hyperledger.besu.evm.gascalculator.ShanghaiGasCalculator;
import org.hyperledger.besu.evm.gascalculator.SpuriousDragonGasCalculator;
import org.hyperledger.besu.evm.gascalculator.TangerineWhistleGasCalculator;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.OptionalLong;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.ExternalProfiler;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.profile.ProfilerException;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;
import org.openjdk.jmh.runner.IterationType;

/**
 * JMH {@link InternalProfiler} that publishes the benchmark's gas throughput as a secondary metric
 * named {@code mgas_per_s} in MGas/s, derived from each measurement iteration's primary score and
 * the gas cost computed with Besu's own {@link GasCalculator}.
 *
 * <p>Run with: {@code ./gradlew :ethereum:core:jmh -PgasProfiler=true}
 *
 * <p>To specify the EVM fork (defaults to OSAKA): {@code ./gradlew :ethereum:core:jmh
 * -PgasProfiler=true -PgasProfilerFork=CANCUN}
 */
public class GasProfiler implements InternalProfiler, ExternalProfiler {

  private final GasCalculator gasCalculator;
  private final String fork;

  // Field seen by the InternalProfiler - ExternalProfiler side never sees the field as it runs in
  // another JVM instance.
  // Lazily populates the gas cost only once, then only computes gas related results if it is
  // optionally available.
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private OptionalLong trialGasCost;

  public GasProfiler() throws ProfilerException {
    this("");
  }

  public GasProfiler(final String initLine) throws ProfilerException {
    fork =
        (initLine == null || initLine.isBlank())
            ? "OSAKA"
            : initLine.trim().toUpperCase(Locale.ROOT);
    try {
      gasCalculator = gasCalculatorForFork(fork);
    } catch (IllegalArgumentException e) {
      throw new ProfilerException(
          "Unknown fork: " + fork + ". Must be a valid EvmSpecVersion name.");
    }
  }

  private static GasCalculator gasCalculatorForFork(final String fork) {
    return switch (EvmSpecVersion.valueOf(fork)) {
      case FRONTIER -> new FrontierGasCalculator();
      case HOMESTEAD -> new HomesteadGasCalculator();
      case TANGERINE_WHISTLE -> new TangerineWhistleGasCalculator();
      case SPURIOUS_DRAGON -> new SpuriousDragonGasCalculator();
      case BYZANTIUM -> new ByzantiumGasCalculator();
      case CONSTANTINOPLE -> new ConstantinopleGasCalculator();
      case PETERSBURG -> new PetersburgGasCalculator();
      case ISTANBUL -> new IstanbulGasCalculator();
      case BERLIN -> new BerlinGasCalculator();
      case LONDON, PARIS -> new LondonGasCalculator();
      case SHANGHAI -> new ShanghaiGasCalculator();
      case CANCUN -> new CancunGasCalculator();
      case PRAGUE -> new PragueGasCalculator();
      case OSAKA -> new OsakaGasCalculator();
      case AMSTERDAM -> new AmsterdamGasCalculator();
      default -> new OsakaGasCalculator();
    };
  }

  @Override
  public String getDescription() {
    return "Emits per-benchmark 'mgas_per_s' as secondary metric using " + fork + " gas rules";
  }

  @Override
  @SuppressWarnings("OptionalAssignedToNull")
  public void beforeIteration(
      final BenchmarkParams benchmarkParams, final IterationParams iterationParams) {
    if (trialGasCost == null) {
      trialGasCost = GasFormulas.compute(benchmarkParams, gasCalculator);
    }
  }

  @Override
  public Collection<? extends Result<?>> afterIteration(
      final BenchmarkParams benchmarkParams,
      final IterationParams iterationParams,
      final IterationResult result) {
    if (iterationParams.getType() != IterationType.MEASUREMENT || trialGasCost.isEmpty()) {
      return Collections.emptyList();
    }
    final double nsPerOp =
        result.getPrimaryResult().getScore() * benchmarkParams.getTimeUnit().toNanos(1);
    return List.of(
        new ScalarResult(
            "mgas_per_s",
            1000 * trialGasCost.getAsLong() / nsPerOp,
            "MGas/s",
            AggregationPolicy.AVG),
        new ScalarResult("gas", trialGasCost.getAsLong(), "gas", AggregationPolicy.MAX));
  }

  @Override
  public Collection<String> addJVMInvokeOptions(final BenchmarkParams params) {
    return Collections.emptyList();
  }

  @Override
  public Collection<String> addJVMOptions(final BenchmarkParams params) {
    return Collections.emptyList();
  }

  @Override
  public void beforeTrial(final BenchmarkParams benchmarkParams) {
    final OptionalLong gasCost = GasFormulas.compute(benchmarkParams, gasCalculator);
    System.out.printf(
        "\n# GasProfiler: fork=%s%s%n",
        fork, gasCost.isPresent() ? ", gasCost=" + gasCost.getAsLong() : "");
  }

  @Override
  public Collection<? extends Result<?>> afterTrial(
      final BenchmarkResult br, final long pid, final File stdOut, final File stdErr) {
    return Collections.emptyList();
  }

  @Override
  public boolean allowPrintOut() {
    return false;
  }

  @Override
  public boolean allowPrintErr() {
    return false;
  }
}
