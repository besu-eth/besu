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
package org.hyperledger.besu.evmtool;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hyperledger.besu.evmtool.BlockchainTestSubCommand.COMMAND_NAME;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.referencetests.BlockchainReferenceTestCaseSpec;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestProtocolSchedules;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.account.AccountState;
import org.hyperledger.besu.evm.internal.EvmConfiguration.WorldUpdaterMode;
import org.hyperledger.besu.evm.precompile.AbstractBLS12PrecompiledContract;
import org.hyperledger.besu.evm.precompile.AbstractPrecompiledContract;
import org.hyperledger.besu.evm.precompile.KZGPointEvalPrecompiledContract;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder.OpCodeTracerConfig;
import org.hyperledger.besu.evm.tracing.StreamingOperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldView;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.BlockImportTracerProvider;
import org.hyperledger.besu.plugin.services.tracer.BlockAwareOperationTracer;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import org.apache.tuweni.bytes.Bytes32;
import picocli.CommandLine.Command;
import picocli.CommandLine.IExitCodeGenerator;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

/**
 * This class, BlockchainTestSubCommand, is a command-line interface (CLI) command that executes an
 * Ethereum State Test. It implements the Runnable interface, meaning it can be used in a thread of
 * execution.
 *
 * <p>The class is annotated with @CommandLine.Command, which is a PicoCLI annotation that
 * designates this class as a command-line command. The annotation parameters define the command's
 * name, description, whether it includes standard help options, and the version provider.
 *
 * <p>The command's functionality is defined in the run() method, which is overridden from the
 * Runnable interface.
 */
@Command(
    name = COMMAND_NAME,
    description = "Execute an Ethereum Blockchain Test.",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class BlockchainTestSubCommand implements Runnable, IExitCodeGenerator {

  /** Process exit code: 0 when all executed tests pass, 1 when any failed or nothing ran. */
  private volatile int exitCode = 0;

  /**
   * The name of the command for the BlockchainTestSubCommand. This constant is used as the name
   * parameter in the @CommandLine.Command annotation. It defines the command name that users should
   * enter on the command line to invoke this command.
   */
  public static final String COMMAND_NAME = "block-test";

  @Option(
      names = {"--test-name"},
      description =
          "Limit execution to tests whose name contains the given substring, or matches the given"
              + " pattern (a regex, with * and ? as wildcards).")
  private String testName = null;

  @Option(
      names = {"--test-name-regex"},
      description =
          "Limit execution to tests whose id matches the given regex, taken verbatim. This is a"
              + " hive --sim.limit value: anchored at the start of the id, open at the end and"
              + " case-sensitive, as re.match is. Nothing is escaped or rewritten, so a published"
              + " hive filter can be passed exactly as it appears.")
  private String testNameRegex = null;

  // Compiled up front so a malformed expression fails before any fixture is read
  private TestNameFilter nameFilter;

  @Option(
      names = {"--trace-output"},
      description = "Output file for traces (default: stderr). Requires --json or --trace flag.")
  private String traceOutput = null;

  @Option(
      names = {"--workers"},
      description =
          "Number of parallel workers for processing fixture files. Defaults to the number of"
              + " available cores (${DEFAULT-VALUE} here); lower it to run with less parallelism.")
  private int workers = Runtime.getRuntime().availableProcessors();

  @Option(
      names = {"--json-array"},
      description = "Output results as a JSON array: name, pass, fork, lastBlockHash, error.")
  private boolean jsonArray = false;

  private final List<ObjectNode> jsonArrayResults = Collections.synchronizedList(new ArrayList<>());

  @Option(
      names = {"--cache-precompiles"},
      description =
          "Enable precompile result caching, matching the runtime behavior of `--cache-precompiles` in besu.",
      negatable = true)
  private Boolean enablePrecompileCache = false;

  @Option(
      names = {"--verbose"},
      description = "Verbose logs, listing all skipped tests")
  private final Boolean verbose = false;

  @ParentCommand private final EvmToolCommand parentCommand;

  // picocli does it magically
  @Parameters private final List<Path> blockchainTestFiles = new ArrayList<>();

  /**
   * Default constructor for the BlockchainTestSubCommand class. This constructor doesn't take any
   * arguments and initializes the parentCommand to null. PicoCLI requires this constructor.
   */
  @SuppressWarnings("unused")
  public BlockchainTestSubCommand() {
    // PicoCLI requires this
    this(null);
  }

  BlockchainTestSubCommand(final EvmToolCommand parentCommand) {
    this.parentCommand = parentCommand;
  }

  @Override
  public int getExitCode() {
    return exitCode;
  }

  @Override
  public void run() {
    AbstractPrecompiledContract.setPrecompileCaching(enablePrecompileCache);
    AbstractBLS12PrecompiledContract.setPrecompileCaching(enablePrecompileCache);
    KZGPointEvalPrecompiledContract.setPrecompileCaching(enablePrecompileCache);
    final ObjectMapper blockchainTestMapper = JsonUtils.createObjectMapper();
    final FixtureRunner.TestResults results = new FixtureRunner.TestResults();

    final JavaType javaType =
        blockchainTestMapper
            .getTypeFactory()
            .constructParametricType(
                Map.class, String.class, BlockchainReferenceTestCaseSpec.class);
    if (testName != null || testNameRegex != null) {
      try {
        nameFilter = TestNameFilter.fromOptions(testName, testNameRegex);
      } catch (final IllegalArgumentException e) {
        parentCommand.out.println(e.getMessage());
        exitCode = 1;
        return;
      }
    }

    boolean setupFailed = false;
    try {
      if (blockchainTestFiles.isEmpty()) {
        // if no files were specified, use standard input to get filenames
        final BufferedReader in =
            new BufferedReader(new InputStreamReader(parentCommand.in, UTF_8));
        while (true) {
          final String fileName = in.readLine();
          if (fileName == null) {
            break;
          }
          final File file = new File(fileName);
          if (file.isFile()) {
            final Map<String, BlockchainReferenceTestCaseSpec> blockchainTests =
                blockchainTestMapper.readValue(file, javaType);
            executeBlockchainTest(blockchainTests, results);
          } else {
            parentCommand.out.println("File not found: " + fileName);
          }
        }
      } else {
        FixtureRunner.runFiles(
            FixtureRunner.collectFiles(blockchainTestFiles),
            workers,
            file -> runFile(file, blockchainTestMapper, javaType, results));
      }
    } catch (final JsonProcessingException jpe) {
      setupFailed = true;
      parentCommand.out.println("File content error: " + jpe);
    } catch (final IOException e) {
      setupFailed = true;
      System.err.println("Unable to read state file: " + e.getMessage());
    } catch (final InterruptedException e) {
      // Catching it cleared the flag; restore it so whoever interrupted the run still sees it.
      Thread.currentThread().interrupt();
      setupFailed = true;
      System.err.println("Interrupted while running blockchain tests");
    } catch (final Exception e) {
      setupFailed = true;
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace(System.err);
    } finally {
      // Fail an empty run, so a typo in --test-name or a fixture tree that did not materialise
      // cannot be mistaken for a clean sweep. Not printed under --json-array, where that output is
      // parsed and only the array belongs.
      if (!results.hasTests() && !jsonArray) {
        parentCommand.out.printf(
            "No blockchain test was executed%s.%n",
            TestNameFilter.describe(testName, testNameRegex));
      }
      exitCode = results.failed() > 0 || setupFailed || !results.hasTests() ? 1 : 0;
      if (jsonArray) {
        FixtureRunner.printJsonArray(parentCommand.out, jsonArrayResults);
      } else if (results.hasTests()) {
        results.printSummary(parentCommand.out);
      }
    }
  }

  private void executeBlockchainTest(
      final Map<String, BlockchainReferenceTestCaseSpec> blockchainTests,
      final FixtureRunner.TestResults results) {
    final Map<String, BlockchainReferenceTestCaseSpec> filteredTests =
        blockchainTests.entrySet().stream()
            .filter(
                entry -> {
                  final String test = entry.getKey();
                  // Gate on the compiled filter, not on --test-name: --test-name-regex compiles one
                  // too, and testing the wrong field silently runs the whole tree unfiltered.
                  if (nameFilter != null && !matchesTestName(test)) {
                    if (verbose) {
                      parentCommand.out.println("Skipping test: " + test);
                    }
                    return false;
                  }
                  if (verbose) {
                    parentCommand.out.println("Considering " + test);
                  }
                  return true;
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    int repeatCount = Math.max(1, parentCommand.getRepeatCount());
    for (int i = 0; i < repeatCount; i++) {
      boolean isLastIteration = (i == repeatCount - 1);
      parentCommand.out.println("Running iteration " + i);
      filteredTests.forEach(
          (testName, spec) -> traceTestSpecs(testName, spec, results, isLastIteration));
    }
  }

  /**
   * Reads one fixture file and runs it. Used by both the sequential and the parallel path, so a
   * directory yields the same counts whatever {@code --workers} is.
   *
   * <p>A file that cannot be read is recorded as unreadable rather than as a test failure.
   */
  private void runFile(
      final Path file,
      final ObjectMapper mapper,
      final JavaType javaType,
      final FixtureRunner.TestResults results) {
    final Map<String, BlockchainReferenceTestCaseSpec> blockchainTests;
    try {
      blockchainTests =
          "stdin".equals(file.toString())
              ? mapper.readValue(parentCommand.in, javaType)
              : mapper.readValue(file.toFile(), javaType);
    } catch (final Exception e) {
      results.recordUnreadable(file.toString(), "not readable as a blockchain test fixture: " + e);
      return;
    }
    executeBlockchainTest(blockchainTests, results);
  }

  private boolean matchesTestName(final String test) {
    return nameFilter.matches(test);
  }

  private void traceTestSpecs(
      final String test,
      final BlockchainReferenceTestCaseSpec spec,
      final FixtureRunner.TestResults results,
      final boolean isLastIteration) {
    if (isLastIteration) {
      parentCommand.out.println("Running " + test);
    }
    final MutableBlockchain blockchain = spec.buildBlockchain();
    final ProtocolContext context =
        spec.buildProtocolContext(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG, blockchain);

    final BlockHeader genesisBlockHeader = spec.getGenesisBlockHeader();
    final MutableWorldState worldState =
        context
            .getWorldStateArchive()
            .getWorldState(
                WorldStateQueryParams.withStateRootAndBlockHashAndUpdateNodeHead(
                    genesisBlockHeader.getStateRoot(), genesisBlockHeader.getHash()))
            .orElseThrow();

    // The fixture's own config.blobSchedule is passed through, because a devnet sets blob target
    // and max to values Besu's defaults do not carry. Build the schedule without it and every blob
    // test is validated against the wrong parameters.
    final ProtocolSchedule schedule =
        ReferenceTestProtocolSchedules.cached(
                parentCommand.getEvmConfiguration(), spec.getBlobScheduleOptions().orElse(null))
            .getByName(spec.getNetwork());

    BlockTestTracerManager tracerManager = null;
    PrintStream traceWriter;
    long totalGasUsed = 0;
    int totalTxCount = 0;
    int blockCount = 0;
    long testStartTime = System.currentTimeMillis();

    boolean testPassed = true;
    String failureReason = "";

    if (parentCommand.showJsonResults && isLastIteration) {
      try {
        final boolean isFileOutput = traceOutput != null;
        if (isFileOutput) {
          traceWriter = new PrintStream(new FileOutputStream(traceOutput, true), true, UTF_8);
        } else {
          traceWriter = new PrintStream(System.err, true, UTF_8);
        }
        tracerManager =
            new BlockTestTracerManager(
                traceWriter,
                parentCommand.showMemory,
                !parentCommand.hideStack,
                parentCommand.showReturnData,
                parentCommand.showStorage);

        final ServiceManager serviceManager = context.getPluginServiceManager();
        final BlockchainTestTracerProvider tracerProvider =
            new BlockchainTestTracerProvider(tracerManager);
        serviceManager.addService(BlockImportTracerProvider.class, tracerProvider);
      } catch (final IOException e) {
        parentCommand.out.println("Failed to open trace output: " + e.getMessage());
        return;
      }
    }

    for (final BlockchainReferenceTestCaseSpec.CandidateBlock candidateBlock :
        spec.getCandidateBlocks()) {
      if (!candidateBlock.isExecutable()) {
        return;
      }

      try {
        final Block block = candidateBlock.getBlock();
        blockCount++;

        final ProtocolSpec protocolSpec = schedule.getByBlockHeader(block.getHeader());
        final BlockImporter blockImporter = protocolSpec.getBlockImporter();

        verifyJournaledEVMAccountCompatability(worldState, protocolSpec);

        final HeaderValidationMode validationMode =
            "NoProof".equalsIgnoreCase(spec.getSealEngine())
                ? HeaderValidationMode.LIGHT
                : HeaderValidationMode.FULL;

        final Stopwatch timer = Stopwatch.createStarted();

        final BlockImportResult importResult =
            blockImporter.importBlock(context, block, validationMode, validationMode);

        timer.stop();

        if (!isLastIteration) {
          continue;
        }

        if (parentCommand.showJsonResults) {
          totalGasUsed += block.getHeader().getGasUsed();
          totalTxCount += block.getBody().getTransactions().size();
        }

        if (importResult.isImported() != candidateBlock.isValid()) {
          testPassed = false;
          failureReason =
              String.format(
                  "Block %d (%s) %s",
                  block.getHeader().getNumber(),
                  block.getHash(),
                  importResult.isImported() ? "Failed to be rejected" : "Failed to import");
          parentCommand.out.println(failureReason);
        } else {
          if (importResult.isImported()) {
            final long gasUsed = block.getHeader().getGasUsed();
            final long timeNs = timer.elapsed(TimeUnit.NANOSECONDS);
            final float mGps = gasUsed * 1000.0f / timeNs;
            final double timeMs = timeNs / 1_000_000.0;
            parentCommand.out.printf(
                "Block %d (%s) Imported in %.2f ms (%.2f MGas/s)%n",
                block.getHeader().getNumber(), block.getHash(), timeMs, mGps);
          } else {
            parentCommand.out.printf(
                "Block %d (%s) Rejected (correctly)%n",
                block.getHeader().getNumber(), block.getHash());
          }
        }
      } catch (final RLPException e) {
        if (candidateBlock.isValid()) {
          testPassed = false;
          failureReason =
              String.format(
                  "Block %d (%s) RLP exception: %s",
                  candidateBlock.getBlock().getHeader().getNumber(),
                  candidateBlock.getBlock().getHash(),
                  e.getMessage());
          parentCommand.out.println(failureReason);
        }
      }
    }

    if (!isLastIteration) {
      return;
    }

    if (!blockchain.getChainHeadHash().equals(spec.getLastBlockHash())) {
      testPassed = false;
      failureReason =
          String.format(
              "Chain header mismatch, have %s want %s",
              blockchain.getChainHeadHash(), spec.getLastBlockHash());
      parentCommand.out.printf(
          "Chain header mismatch, have %s want %s%n",
          blockchain.getChainHeadHash(), spec.getLastBlockHash());
    } else {
      if (verbose) {
        parentCommand.out.println("Chain import successful");
      }
    }

    if (parentCommand.showJsonResults) {
      final long testDuration = System.currentTimeMillis() - testStartTime;
      tracerManager.writeTestEnd(
          test,
          testPassed,
          spec.getNetwork(),
          testDuration,
          totalGasUsed,
          totalTxCount,
          blockCount);
    }

    if (!testPassed) {
      results.recordFailure(test, failureReason);
    } else {
      results.recordPass();
    }

    if (jsonArray) {
      final ObjectNode result = FixtureRunner.newResultNode();
      result.put("name", test);
      result.put("pass", testPassed);
      result.put("fork", spec.getNetwork());
      result.put("lastBlockHash", blockchain.getChainHeadHash().toHexString());
      result.put("error", failureReason);
      jsonArrayResults.add(result);
    }
  }

  void verifyJournaledEVMAccountCompatability(
      final MutableWorldState worldState, final ProtocolSpec protocolSpec) {
    EVM evm = protocolSpec.getEvm();
    if (evm.getEvmConfiguration().worldUpdaterMode() == WorldUpdaterMode.JOURNALED) {
      if (worldState
          .streamAccounts(Bytes32.ZERO, Integer.MAX_VALUE)
          .anyMatch(AccountState::isEmpty)) {
        parentCommand.out.println("Journaled account configured and empty account detected");
      }

      if (EvmSpecVersion.SPURIOUS_DRAGON.compareTo(evm.getEvmVersion()) > 0) {
        parentCommand.out.println(
            "Journaled account configured and fork prior to the merge specified");
      }
    }
  }

  /**
   * Implementation of BlockImportTracerProvider that provides BlockAwareOperationTracer instances
   * for block import tracing. This class bridges the BlockTestTracerManager with Besu's standard
   * BlockImportTracerProvider infrastructure.
   */
  private record BlockchainTestTracerProvider(BlockTestTracerManager tracerManager)
      implements BlockImportTracerProvider {

    @Override
    public BlockAwareOperationTracer getBlockImportTracer(
        final org.hyperledger.besu.plugin.data.BlockHeader blockHeader) {
      return new BlockchainTestTracer(tracerManager.createTracer());
    }
  }

  /**
   * Adapter that wraps a StreamingOperationTracer and implements BlockAwareOperationTracer. This
   * adapter delegates all OperationTracer method calls to the underlying StreamingOperationTracer
   * while providing default implementations for block-level tracing methods.
   */
  private record BlockchainTestTracer(StreamingOperationTracer delegate)
      implements BlockAwareOperationTracer {

    @Override
    public void tracePrepareTransaction(
        final WorldView worldView, final org.hyperledger.besu.datatypes.Transaction transaction) {
      delegate.tracePrepareTransaction(worldView, transaction);
    }

    @Override
    public void traceStartTransaction(
        final WorldView worldView, final org.hyperledger.besu.datatypes.Transaction transaction) {
      delegate.traceStartTransaction(worldView, transaction);
    }

    @Override
    public void traceEndTransaction(
        final WorldView worldView,
        final org.hyperledger.besu.datatypes.Transaction tx,
        final boolean status,
        final org.apache.tuweni.bytes.Bytes output,
        final List<Log> logs,
        final long gasUsed,
        final Set<Address> selfDestructs,
        final long timeNs) {
      delegate.traceEndTransaction(
          worldView, tx, status, output, logs, gasUsed, selfDestructs, timeNs);
    }

    @Override
    public void traceBeforeRewardTransaction(
        final WorldView worldView,
        final org.hyperledger.besu.datatypes.Transaction transaction,
        final Wei miningReward) {
      delegate.traceBeforeRewardTransaction(worldView, transaction, miningReward);
    }

    @Override
    public void traceContextEnter(final org.hyperledger.besu.evm.frame.MessageFrame frame) {
      delegate.traceContextEnter(frame);
    }

    @Override
    public void traceContextReEnter(final org.hyperledger.besu.evm.frame.MessageFrame frame) {
      delegate.traceContextReEnter(frame);
    }

    @Override
    public void traceContextExit(final org.hyperledger.besu.evm.frame.MessageFrame frame) {
      delegate.traceContextExit(frame);
    }

    @Override
    public void tracePreExecution(final org.hyperledger.besu.evm.frame.MessageFrame frame) {
      delegate.tracePreExecution(frame);
    }

    @Override
    public void tracePostExecution(
        final org.hyperledger.besu.evm.frame.MessageFrame frame,
        final org.hyperledger.besu.evm.operation.Operation.OperationResult operationResult) {
      delegate.tracePostExecution(frame, operationResult);
    }

    @Override
    public void tracePrecompileCall(
        final org.hyperledger.besu.evm.frame.MessageFrame frame,
        final long gasRequirement,
        final org.apache.tuweni.bytes.Bytes output) {
      delegate.tracePrecompileCall(frame, gasRequirement, output);
    }

    @Override
    public void traceAccountCreationResult(
        final org.hyperledger.besu.evm.frame.MessageFrame frame,
        final java.util.Optional<org.hyperledger.besu.evm.frame.ExceptionalHaltReason> haltReason) {
      delegate.traceAccountCreationResult(frame, haltReason);
    }

    @Override
    public boolean isExtendedTracing() {
      return true;
    }
  }

  /**
   * Inner class to manage tracing for blockchain tests. This class encapsulates the logic for
   * creating and managing StreamingOperationTracer instances for transaction-level tracing during
   * block test execution.
   *
   * <p>Note: Trace output is separated from test execution status output to ensure JSONL purity.
   * Test status messages go to stdout (parentCommand.out), while traces go to stderr or a file to
   * prevent mixing human-readable messages with machine-parseable JSONL trace data.
   */
  private static class BlockTestTracerManager {
    private final PrintStream output;
    private final boolean showMemory;
    private final boolean showStack;
    private final boolean showReturnData;
    private final boolean showStorage;
    private StreamingOperationTracer currentTracer;

    /**
     * Constructs a BlockTestTracerManager with specified tracing options.
     *
     * @param output the PrintWriter for trace output
     * @param showMemory whether to include memory in traces
     * @param showStack whether to include stack in traces
     * @param showReturnData whether to include return data in traces
     * @param showStorage whether to include storage changes in traces
     */
    public BlockTestTracerManager(
        final PrintStream output,
        final boolean showMemory,
        final boolean showStack,
        final boolean showReturnData,
        final boolean showStorage) {
      this.output = output;
      this.showMemory = showMemory;
      this.showStack = showStack;
      this.showReturnData = showReturnData;
      this.showStorage = showStorage;
    }

    /**
     * Creates a new StreamingOperationTracer for tracing a transaction.
     *
     * @return a new StreamingOperationTracer instance
     */
    public StreamingOperationTracer createTracer() {
      currentTracer =
          new StreamingOperationTracer(
              output,
              OpCodeTracerConfigBuilder.createFrom(OpCodeTracerConfig.DEFAULT)
                  .traceMemory(showMemory)
                  .traceStack(showStack)
                  .traceReturnData(showReturnData)
                  .traceStorage(showStorage)
                  .eip3155Strict(true)
                  .build());
      return currentTracer;
    }

    /**
     * Writes a test end marker with test execution metrics.
     *
     * @param testName the name of the test
     * @param passed whether the test passed
     * @param fork the fork name
     * @param durationMs test duration in milliseconds
     * @param gasUsed total gas used
     * @param txCount transaction count
     * @param blockCount block count
     */
    public void writeTestEnd(
        final String testName,
        final boolean passed,
        final String fork,
        final long durationMs,
        final long gasUsed,
        final int txCount,
        final int blockCount) {
      output.printf(
          "{\"test\":\"%s\",\"pass\":%s,\"fork\":\"%s\",\"duration\":%d,"
              + "\"gasUsed\":%d,\"txCount\":%d,\"blockCount\":%d}%n",
          testName, passed, fork, durationMs, gasUsed, txCount, blockCount);
      output.flush();
    }
  }
}
