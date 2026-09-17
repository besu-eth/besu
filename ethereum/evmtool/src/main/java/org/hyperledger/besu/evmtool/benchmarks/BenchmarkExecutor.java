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
package org.hyperledger.besu.evmtool.benchmarks;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.fluent.SimpleBlockValues;
import org.hyperledger.besu.evm.fluent.SimpleWorld;
import org.hyperledger.besu.evm.frame.MessageFrame;
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
import org.hyperledger.besu.evm.precompile.PrecompiledContract;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.SequencedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import one.profiler.AsyncProfiler;
import org.apache.tuweni.bytes.Bytes;

/** Abstract class to support benchmarking of various client algorithms */
public abstract class BenchmarkExecutor {

  private static final int MAX_EXEC_TIME_IN_SECONDS = 1;
  private static final int MAX_WARMUP_TIME_IN_SECONDS = 3;
  private static final long GAS_PER_SECOND_STANDARD = 100_000_000L;

  static final int MATH_WARMUP = 20_000;
  static final int MATH_ITERATIONS = 1_000;

  /** Where to write the output of the benchmarks. */
  protected final PrintStream output;

  private final BenchmarkConfig config;

  private Runnable precompileTableHeader;
  int warmIterations;
  private final long warmTimeInNano;
  int execIterations;
  private final long execTimeInNano;

  static final MessageFrame fakeFrame =
      MessageFrame.builder()
          .type(MessageFrame.Type.CONTRACT_CREATION)
          .contract(Address.ZERO)
          .inputData(Bytes.EMPTY)
          .sender(Address.ZERO)
          .value(Wei.ZERO)
          .apparentValue(Wei.ZERO)
          .code(Code.EMPTY_CODE)
          .completer(__ -> {})
          .address(Address.ZERO)
          .blockHashLookup((__, ___) -> null)
          .blockValues(new SimpleBlockValues())
          .gasPrice(Wei.ZERO)
          .miningBeneficiary(Address.ZERO)
          .originator(Address.ZERO)
          .initialGas(100_000L)
          .worldUpdater(new SimpleWorld())
          .build();

  /**
   * Run benchmarks with specified warmup and iterations
   *
   * @param warmup number of executions to run before timing
   * @param iterations number of executions to time.
   * @param output print stream to print the output to.
   * @param benchmarkConfig options to give to the benchmark runner.
   */
  public BenchmarkExecutor(
      final int warmup,
      final int iterations,
      final PrintStream output,
      final BenchmarkConfig benchmarkConfig) {
    this.warmIterations =
        benchmarkConfig
            .warmIterations()
            .orElseGet(() -> benchmarkConfig.warmTime().isEmpty() ? warmup : Integer.MAX_VALUE);
    this.warmTimeInNano =
        TimeUnit.SECONDS.toNanos(
            benchmarkConfig
                .warmTime()
                .orElseGet(
                    () ->
                        benchmarkConfig.warmIterations().isEmpty()
                            ? MAX_WARMUP_TIME_IN_SECONDS
                            : Integer.MAX_VALUE));
    this.execIterations =
        benchmarkConfig
            .execIterations()
            .orElseGet(() -> benchmarkConfig.execTime().isEmpty() ? iterations : Integer.MAX_VALUE);
    this.execTimeInNano =
        TimeUnit.SECONDS.toNanos(
            benchmarkConfig
                .execTime()
                .orElseGet(
                    () ->
                        benchmarkConfig.warmIterations().isEmpty()
                            ? MAX_EXEC_TIME_IN_SECONDS
                            : Integer.MAX_VALUE));
    this.output = output;
    this.precompileTableHeader =
        () -> {
          if (benchmarkConfig.attemptCacheBust())
            output.println("--attempt-cache-bust=true (--warm-time and --exec-time ignored)");
          output.printf("--warm-iterations=%d%n", warmIterations);
          if (!benchmarkConfig.attemptCacheBust())
            output.printf("--warm-time=%ss%n", warmTimeInNano / 1.0e9D);
          output.printf("--exec-iterations=%d%n", execIterations);
          if (!benchmarkConfig.attemptCacheBust())
            output.printf("--exec-time=%ss%n", execTimeInNano / 1.0e9D);
          output.printf(
              "%-30s | %12s | %12s | %15s | %15s%n",
              "", "Actual cost", "Derived Cost", "Iteration time", "Throughput");
        };
    this.config = benchmarkConfig;
    assert warmIterations >= 0;
    assert execIterations >= 0;
  }

  /**
   * Benchmarks the given precompile with all the test cases provided. This method selectively runs
   * the benchmark and/or particular test cases accordingly with the CLI options that were provided.
   *
   * @param testCases all test cases to run against the precompile.
   * @param contract precompile contract to execute.
   * @param evmSpecVersion EVM specification version to run the precompile for.
   */
  public void precompile(
      final SequencedMap<String, Bytes> testCases,
      final PrecompiledContract contract,
      final EvmSpecVersion evmSpecVersion) {

    if (contract == null) {
      throw new UnsupportedOperationException(
          "contract is unsupported on " + evmSpecVersion + " fork");
    }

    Optional<Pattern> maybePattern = config.testCasePattern().map(Pattern::compile);
    LinkedHashMap<String, Bytes> filteredTestCases = new LinkedHashMap<>();
    testCases.forEach(
        (k, v) -> {
          if (maybePattern.map(p -> p.matcher(k).find()).orElse(true)) {
            filteredTestCases.put(k, v);
          }
        });

    if (config.attemptCacheBust()) {
      runPrecompileAttemptCacheBust(filteredTestCases, contract);
    } else {
      runPrecompile(filteredTestCases, contract);
    }
  }

  private void runPrecompileAttemptCacheBust(
      final Map<String, Bytes> testCases, final PrecompiledContract contract) {

    // Warmup all test cases in serial inside one warmup iteration
    // avoid using warmTime as it is now dependent on the number of test cases
    for (int i = 0; i < warmIterations; i++) {
      for (final Map.Entry<String, Bytes> testCase : testCases.entrySet()) {
        contract.computePrecompile(testCase.getValue(), fakeFrame);
      }
    }

    // Also run all test cases in serial inside one iteration
    Map<String, Long> totalElapsedByTestName = new HashMap<>();
    int executions = 0;
    while (executions < execIterations) {
      for (final Map.Entry<String, Bytes> testCase : testCases.entrySet()) {
        final long iterationStart = System.nanoTime();
        final var result = contract.computePrecompile(testCase.getValue(), fakeFrame);
        final long iterationElapsed = System.nanoTime() - iterationStart;
        if (result.output() != null) {
          // adds iterationElapsed if absent, or sums with existing value
          totalElapsedByTestName.merge(testCase.getKey(), iterationElapsed, Long::sum);
        } else {
          throw new IllegalArgumentException("Input is Invalid for " + testCase.getValue());
        }
      }
      executions++;
    }

    for (final Map.Entry<String, Bytes> testCase : testCases.entrySet()) {
      if (totalElapsedByTestName.containsKey(testCase.getKey())) {
        final double execTime =
            totalElapsedByTestName.get(testCase.getKey()) / 1.0e9D / execIterations;
        // log the performance of the precompile
        long gasCost = contract.gasRequirement(testCases.get(testCase.getKey()));
        logPrecompilePerformance(testCase.getKey(), gasCost, execTime);
      } else {
        output.printf("%s Test case missing from results%n", testCase.getKey());
      }
    }
  }

  private void runPrecompile(
      final Map<String, Bytes> testCases, final PrecompiledContract contract) {

    // Fully warmup and execute, test case by test case
    for (final Map.Entry<String, Bytes> testCase : testCases.entrySet()) {
      final double execTime =
          runPrecompileBenchmark(testCase.getKey(), testCase.getValue(), contract);
      long gasCost = contract.gasRequirement(testCase.getValue());
      logPrecompilePerformance(testCase.getKey(), gasCost, execTime);
    }
  }

  /**
   * Run the benchmark with the specific args. Execution will be done warmup + iterations times
   *
   * @param testName name of the test execution for the async profiler if configured
   * @param arg the bytes arguments to pass into the contract
   * @param contract the precompiled contract to benchmark
   * @return the mean number of seconds each timed iteration took.
   */
  protected double runPrecompileBenchmark(
      final String testName, final Bytes arg, final PrecompiledContract contract) {
    if (contract.computePrecompile(arg, fakeFrame).output() == null) {
      throw new IllegalArgumentException("Input is Invalid for " + testName);
    }

    // Warmup individual test case fully, which may have side effect of warming cpu caches
    long startWarmNanoTime = System.nanoTime();
    for (int i = 0;
        i < warmIterations && System.nanoTime() - startWarmNanoTime < warmTimeInNano;
        i++) {
      contract.computePrecompile(arg, fakeFrame);
    }

    // Iterations
    final AtomicReference<AsyncProfiler> asyncProfiler = new AtomicReference<>();
    config
        .asyncProfilerOptions()
        .ifPresent(
            options -> {
              asyncProfiler.set(AsyncProfiler.getInstance());
              try {
                asyncProfiler
                    .get()
                    .execute(processProfilerArgs(options, testName.replaceAll("\\s", "-")));
              } catch (Throwable t) {
                output.println("async profiler unavailable: " + t.getMessage());
              }
            });

    int executions = 0;
    long totalElapsed = 0;
    while (executions < execIterations && totalElapsed < execTimeInNano) {
      long iterationStart = System.nanoTime();
      contract.computePrecompile(arg, fakeFrame);
      long iterationElapsed = System.nanoTime() - iterationStart;

      totalElapsed += iterationElapsed;
      executions++;
    }

    if (asyncProfiler.get() != null) {
      try {
        asyncProfiler.get().stop();
      } catch (Throwable t) {
        output.println("async profiler unavailable: " + t.getMessage());
      }
    }

    return totalElapsed / 1.0e9D / executions;
  }

  /**
   * Logs performance numbers of precompiles. Should not be called outside of this class unless a
   * custom precompile run is preferred.
   *
   * @param testCase name of the test case
   * @param gasCost cost it takes for the given test case to run with the given precompile
   * @param execTime elapsed time of a single iteration
   */
  protected void logPrecompilePerformance(
      final String testCase, final long gasCost, final double execTime) {
    double derivedGas = execTime * GAS_PER_SECOND_STANDARD;

    precompileTableHeader.run();
    output.printf(
        "%-30s | %,8d gas | %,8.0f gas | %,12.1f ns | %,10.2f MGps%n",
        testCase, gasCost, derivedGas, execTime * 1_000_000_000, gasCost / execTime / 1_000_000);
    precompileTableHeader = () -> {};
  }

  /**
   * Return the gas calculator at a given fork. Some forks don't have a specific gas calculator and
   * will return the prior one
   *
   * @param fork name of the fork
   * @return a gas calculator
   */
  public static GasCalculator gasCalculatorForFork(final String fork) {
    return switch (EvmSpecVersion.valueOf(fork.toUpperCase(Locale.ROOT))) {
      case HOMESTEAD -> new HomesteadGasCalculator();
      case FRONTIER -> new FrontierGasCalculator();
      case TANGERINE_WHISTLE -> null;
      case SPURIOUS_DRAGON -> null;
      case BYZANTIUM -> new ByzantiumGasCalculator();
      case CONSTANTINOPLE -> new ConstantinopleGasCalculator();
      case PETERSBURG -> new PetersburgGasCalculator();
      case ISTANBUL -> new IstanbulGasCalculator();
      case BERLIN -> new BerlinGasCalculator();
      case LONDON, PARIS -> new LondonGasCalculator();
      case SHANGHAI -> new ShanghaiGasCalculator();
      case CANCUN -> new CancunGasCalculator();
      case PRAGUE -> new PragueGasCalculator();
      case OSAKA, AMSTERDAM, BOGOTA, POLIS, BANGKOK, FUTURE_EIPS, EXPERIMENTAL_EIPS ->
          new OsakaGasCalculator();
    };
  }

  /**
   * Run the benchmarks
   *
   * @param attemptNative Should the benchmark attempt to us native libraries? (null use the
   *     default, false disabled, true enabled)
   * @param fork the fork name to run the benchmark against.
   */
  // TODO: remove attemptNative since it's already available from BenchmarkConfig here
  public abstract void runBenchmark(final Boolean attemptNative, final String fork);

  /**
   * Little disclaimer about how derived gas is computed for Precompiles.
   *
   * @param output print stream to print the output to.
   */
  public static void logPrecompileDerivedGasNotice(final PrintStream output) {
    long executionTimeExampleNs = 247_914L;
    long gasPerSecond = GAS_PER_SECOND_STANDARD;
    long derivedGas = (executionTimeExampleNs * gasPerSecond) / 1_000_000_000L;

    output.println(
        "\n**** Calculate the derived gas from execution time with a target of 100 mgas/s *****");
    output.println(
        "*                                                                                  *");
    output.println(
        "*   If "
            + String.format("%,d", executionTimeExampleNs)
            + " ns is the execution time of the precompile call, so this is how     *");
    output.println(
        "*                the derived gas is calculated                                     *");
    output.println(
        "*                                                                                  *");
    output.println(
        "*   "
            + String.format("%,d", gasPerSecond)
            + " gas    -------> 1 second (1_000_000_000 ns)                        *");
    output.println(
        "*   x           gas    -------> "
            + String.format("%,d", executionTimeExampleNs)
            + " ns                                         *");
    output.println(
        "*                                                                                  *");
    output.println(
        "*\tx = ("
            + String.format("%,d", executionTimeExampleNs)
            + " * "
            + String.format("%,d", gasPerSecond)
            + ") / 1_000_000_000 = "
            + String.format("%,d", derivedGas)
            + " gas"
            + "                   *");
    output.println(
        "************************************************************************************\n");
  }

  /**
   * Check if this is a Precompile.
   *
   * @return true if this is a benchmark concerning a Precompile, false otherwise
   */
  public boolean isPrecompile() {
    return false;
  }

  /** Interface in how to construct a BenchmarkExecutor statically. */
  @FunctionalInterface
  public interface Builder {
    /**
     * Creates a new BenchmarkExecutor.
     *
     * @param output where to write the stats.
     * @param asyncProfilerOptions starting options for the AsyncProfiler.
     * @return the newly created executor.
     */
    BenchmarkExecutor create(PrintStream output, BenchmarkConfig asyncProfilerOptions);
  }

  private static String processProfilerArgs(
      final String asyncProfilerOptions, final String testCaseName) {
    String[] args = asyncProfilerOptions.split(",");
    for (int i = 0; i < args.length; i++) {
      if (args[i].contains("file=")) {
        args[i] = args[i].replaceAll("%%test-case", testCaseName);
        break;
      }
    }
    return String.join(",", args);
  }
}
