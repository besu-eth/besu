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
package org.hyperledger.besu.evmtool;

import static org.hyperledger.besu.evmtool.BenchmarkSubCommand.COMMAND_NAME;
import static picocli.CommandLine.ScopeType.INHERIT;
import static picocli.CommandLine.ScopeType.LOCAL;

import org.hyperledger.besu.evm.precompile.AbstractBLS12PrecompiledContract;
import org.hyperledger.besu.evm.precompile.AbstractPrecompiledContract;
import org.hyperledger.besu.evmtool.benchmarks.AltBN128Benchmark;
import org.hyperledger.besu.evmtool.benchmarks.BLS12Benchmark;
import org.hyperledger.besu.evmtool.benchmarks.BenchmarkConfig;
import org.hyperledger.besu.evmtool.benchmarks.BenchmarkExecutor;
import org.hyperledger.besu.evmtool.benchmarks.ECRecoverBenchmark;
import org.hyperledger.besu.evmtool.benchmarks.KZGPointEvalBenchmark;
import org.hyperledger.besu.evmtool.benchmarks.ModExpBenchmark;
import org.hyperledger.besu.evmtool.benchmarks.P256VerifyBenchmark;
import org.hyperledger.besu.evmtool.benchmarks.RipeMD160Benchmark;
import org.hyperledger.besu.evmtool.benchmarks.SHA256Benchmark;
import org.hyperledger.besu.util.BesuVersionUtils;
import org.hyperledger.besu.util.LogConfigurator;

import java.io.PrintStream;
import java.util.EnumSet;
import java.util.Optional;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.HardwareAbstractionLayer;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

/**
 * This class represents the BenchmarkSubCommand. It is responsible for executing an Ethereum State
 * Test.
 */
@CommandLine.Command(
    name = COMMAND_NAME,
    description = "Execute an Ethereum State Test.",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class BenchmarkSubCommand implements Runnable {
  /**
   * The command name for the BenchmarkSubCommand. This constant is used as the name attribute in
   * the {@code CommandLine.Command} annotation.
   */
  public static final String COMMAND_NAME = "benchmark";

  /** Stream for where to write the output to. */
  private final PrintStream output;

  enum Benchmark {
    altBn128(AltBN128Benchmark::new),
    // blake2f
    EcRecover(ECRecoverBenchmark::new),
    ModExp(ModExpBenchmark::new),
    // bls12
    Bls12(BLS12Benchmark::new),
    p256Verify(P256VerifyBenchmark::new),
    sha256(SHA256Benchmark::new),
    RipeMD(RipeMD160Benchmark::new),
    kzgPointEval(KZGPointEvalBenchmark::new);

    private final BenchmarkExecutor.Builder executorBuilder;

    Benchmark(final BenchmarkExecutor.Builder executorBuilder) {
      this.executorBuilder = executorBuilder;
    }
  }

  @Option(
      names = {"--native"},
      description = "Use the native libraries.",
      scope = INHERIT,
      negatable = true)
  Boolean nativeCode = false;

  @Option(
      names = {"--use-precompile-cache"},
      description = "Benchmark using precompile caching.",
      scope = INHERIT,
      negatable = true)
  Boolean enablePrecompileCache = false;

  @Option(
      names = {"--async-profiler"},
      description =
          "Benchmark using async profiler. No profiler command means profiling disabled. '%%%%test-case' in the"
              + " file name expands to the test for which the profiler ran,"
              + "e.g. \"start,jfr,event=cpu,file=/tmp/%%%%test-case-%%p.jfr\".",
      scope = LOCAL)
  Optional<String> asyncProfilerOptions = Optional.empty();

  @Option(
      names = {"--pattern"},
      description =
          "Only tests cases with this pattern will be run, e.g. --pattern \"guido-3.*\". Default runs all test cases.",
      scope = LOCAL)
  Optional<String> testCasePattern = Optional.empty();

  @Option(
      names = {"--exec-iterations"},
      description =
          "Number of iterations that the benchmark should run (measurement) for, regardless of how long it takes.",
      scope = LOCAL)
  Optional<Integer> execIterations = Optional.empty();

  @Option(
      names = {"--exec-time"},
      description =
          "Run the maximum number of iterations during execution (measurement) within the given period. Time is in seconds.",
      scope = LOCAL)
  Optional<Integer> execTime = Optional.empty();

  @Option(
      names = {"--warm-iterations"},
      description =
          "Number of iterations that the benchmark should warm up for, regardless of how long it takes.",
      scope = LOCAL)
  Optional<Integer> warmIterations = Optional.empty();

  @Option(
      names = {"--warm-time"},
      description =
          "Run the maximum number of iterations during warmup within the given period. Time is in seconds.",
      scope = LOCAL)
  Optional<Integer> warmTime = Optional.empty();

  @Option(
      names = {"--attempt-cache-bust"},
      description =
          "Run each test case within each warmup and exec iteration. This attempts to warm the code without warming the data, i.e. avoid warming CPU caches. Benchmark must have sufficient number and variety of test cases to be effective. --warm-time, --exec-time and --async-profiler are ignored.",
      scope = LOCAL,
      negatable = true)
  Boolean attemptCacheBust = false;

  @Parameters(description = "One or more of ${COMPLETION-CANDIDATES}.")
  EnumSet<Benchmark> benchmarks = EnumSet.noneOf(Benchmark.class);

  @ParentCommand EvmToolCommand parentCommand;

  /** Default constructor for the BenchmarkSubCommand class. This is required by PicoCLI. */
  public BenchmarkSubCommand() {
    // PicoCLI requires this
    this(System.out);
  }

  /**
   * Constructs a new BenchmarkSubCommand with the given output stream.
   *
   * @param output the output stream to be used
   */
  @SuppressWarnings("NullAway") // Picocli injects the parent command after construction.
  public BenchmarkSubCommand(final PrintStream output) {
    this.output = output;
  }

  @Override
  public void run() {
    LogConfigurator.setLevel("", "DEBUG");
    output.println(BesuVersionUtils.version());
    AbstractPrecompiledContract.setPrecompileCaching(enablePrecompileCache);
    AbstractBLS12PrecompiledContract.setPrecompileCaching(enablePrecompileCache);
    var benchmarksToRun = benchmarks.isEmpty() ? EnumSet.allOf(Benchmark.class) : benchmarks;
    final BenchmarkConfig benchmarkConfig =
        new BenchmarkConfig(
            nativeCode,
            enablePrecompileCache,
            asyncProfilerOptions,
            testCasePattern,
            execIterations,
            execTime,
            warmIterations,
            warmTime,
            attemptCacheBust);
    for (var benchmark : benchmarksToRun) {
      output.println("\nBenchmarks for " + benchmark + " on fork " + parentCommand.getFork());
      BenchmarkExecutor executor = benchmark.executorBuilder.create(output, benchmarkConfig);
      if (executor.isPrecompile()) {
        BenchmarkExecutor.logPrecompileDerivedGasNotice(output);
      }
      executor.runBenchmark(nativeCode, parentCommand.getFork());
    }
    logSystemInfo(output);
  }

  private static void logSystemInfo(final PrintStream output) {
    output.println(
        "\n****************************** Hardware Specs ******************************");
    output.println("*");
    SystemInfo si = new SystemInfo();
    HardwareAbstractionLayer hal = si.getHardware();
    CentralProcessor processor = hal.getProcessor();
    output.println("* OS: " + si.getOperatingSystem());
    output.println("* Processor: " + processor.getProcessorIdentifier().getName());
    output.println(
        "* Microarchitecture: " + processor.getProcessorIdentifier().getMicroarchitecture());
    output.println("* Physical CPU packages: " + processor.getPhysicalPackageCount());
    output.println("* Physical CPU cores: " + processor.getPhysicalProcessorCount());
    output.println("* Logical CPU cores: " + processor.getLogicalProcessorCount());
    output.println(
        "* Average Max Frequency per core: "
            + processor.getMaxFreq() / 100_000 / processor.getLogicalProcessorCount()
            + " MHz");
    output.println("* Memory Total: " + hal.getMemory().getTotal() / 1_000_000_000 + " GB");
  }
}
