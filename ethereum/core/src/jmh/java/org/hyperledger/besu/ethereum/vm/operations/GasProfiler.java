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

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.OptionalLong;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.profile.ExternalProfiler;
import org.openjdk.jmh.profile.ProfilerException;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

/**
 * JMH {@link ExternalProfiler} that publishes the per-benchmark gas cost as a secondary metric
 * named {@code gas}, computed once per trial using Besu's own {@link GasCalculator}.
 *
 * <p>Run with: {@code ./gradlew :ethereum:core:jmh -PgasProfiler=true}
 *
 * <p>To specify the EVM fork (defaults to OSAKA): {@code ./gradlew :ethereum:core:jmh
 * -PgasProfiler=true -PgasProfilerFork=CANCUN}
 */
public class GasProfiler implements ExternalProfiler {

  private final GasCalculator gasCalculator;
  private final String fork;

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
      default -> new OsakaGasCalculator();
    };
  }

  @Override
  public String getDescription() {
    return "Emits per-benchmark gas cost as secondary metric 'gas' using " + fork + " gas rules";
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
  public void beforeTrial(final BenchmarkParams params) {}

  @Override
  public Collection<? extends Result<?>> afterTrial(
      final BenchmarkResult br, final long pid, final File stdOut, final File stdErr) {
    final OptionalLong gas = GasFormulas.compute(br.getParams(), gasCalculator);
    if (gas.isEmpty()) {
      return Collections.emptyList();
    }
    return List.of(new ScalarResult("gas", (double) gas.getAsLong(), "gas", AggregationPolicy.AVG));
  }

  @Override
  public boolean allowPrintOut() {
    return true;
  }

  @Override
  public boolean allowPrintErr() {
    return true;
  }
}
