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
import static org.hyperledger.besu.ethereum.referencetests.ReferenceTestProtocolSchedules.shouldClearEmptyAccounts;
import static org.hyperledger.besu.evmtool.StateTestSubCommand.COMMAND_NAME;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.referencetests.GeneralStateTestCaseEipSpec;
import org.hyperledger.besu.ethereum.referencetests.GeneralStateTestCaseSpec;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestProtocolSchedules;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.tracing.StreamingOperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.evmtool.exception.UnsupportedForkException;
import org.hyperledger.besu.util.LogConfigurator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import org.apache.tuweni.bytes.Bytes;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

/**
 * This class, StateTestSubCommand, is a command-line interface (CLI) command that executes an
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
    description = "Execute an Ethereum State Test.",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class StateTestSubCommand implements Runnable {
  /**
   * The name of the command for the StateTestSubCommand. This constant is used as the name
   * parameter in the @CommandLine.Command annotation. It defines the command name that users should
   * enter on the command line to invoke this command.
   */
  public static final String COMMAND_NAME = "state-test";

  @SuppressWarnings({"FieldCanBeFinal"})
  @Option(
      names = {"--fork"},
      description = "Force the state tests to run on a specific fork.")
  private String fork = null;

  @Option(
      names = {"--test-name"},
      description = "Limit execution to one named test.")
  private String testName = null;

  @Option(
      names = {"--data-index"},
      description = "Limit execution to one data variable.")
  private Integer dataIndex = null;

  @Option(
      names = {"--gas-index"},
      description = "Limit execution to one gas variable.")
  private Integer gasIndex = null;

  @Option(
      names = {"--value-index"},
      description = "Limit execution to one value variable.")
  private Integer valueIndex = null;

  @Option(
      names = {"--fork-index"},
      description = "Limit execution to one fork.")
  private String forkIndex = null;

  @ParentCommand private final EvmToolCommand parentCommand;

  // picocli does it magically
  @Parameters private final List<Path> stateTestFiles = new ArrayList<>();

  /**
   * Default constructor for the StateTestSubCommand class. This constructor doesn't take any
   * arguments and initializes the parentCommand to null. PicoCLI requires this constructor.
   */
  @SuppressWarnings("unused")
  public StateTestSubCommand() {
    // PicoCLI requires this
    this(null);
  }

  StateTestSubCommand(final EvmToolCommand parentCommand) {
    this.parentCommand = parentCommand;
  }

  @Override
  public void run() {
    LogConfigurator.setLevel("", "OFF");
    final ObjectMapper stateTestMapper = JsonUtils.createObjectMapper();

    final JavaType javaType =
        stateTestMapper
            .getTypeFactory()
            .constructParametricType(Map.class, String.class, GeneralStateTestCaseSpec.class);
    try {
      if (stateTestFiles.isEmpty()) {
        // if no state tests were specified use standard input to get filenames
        final BufferedReader in =
            new BufferedReader(new InputStreamReader(parentCommand.in, UTF_8));
        while (true) {
          final String fileName = in.readLine();
          if (fileName == null) {
            // reached end of file.  Stop the loop.
            break;
          }
          final File file = new File(fileName);
          if (file.isFile()) {
            final Map<String, GeneralStateTestCaseSpec> generalStateTests =
                stateTestMapper.readValue(file, javaType);
            executeStateTest(generalStateTests);
          } else {
            parentCommand.out.println("File not found: " + fileName);
          }
        }
      } else {
        for (final Path stateTestFile : stateTestFiles) {
          final Map<String, GeneralStateTestCaseSpec> generalStateTests;
          if ("stdin".equals(stateTestFile.toString())) {
            generalStateTests = stateTestMapper.readValue(parentCommand.in, javaType);
          } else {
            generalStateTests = stateTestMapper.readValue(stateTestFile.toFile(), javaType);
          }
          executeStateTest(generalStateTests);
        }
      }
    } catch (final JsonProcessingException jpe) {
      parentCommand.out.println("File content error: " + jpe);
    } catch (final IOException e) {
      System.err.println("Unable to read state file");
      e.printStackTrace(System.err);
    }
  }

  private void executeStateTest(final Map<String, GeneralStateTestCaseSpec> generalStateTests) {
    int repeatCount = Math.max(1, parentCommand.getRepeatCount());
    for (int i = 0; i < repeatCount; i++) {
      boolean isLastIteration = (i == repeatCount - 1);
      for (final Map.Entry<String, GeneralStateTestCaseSpec> generalStateTestEntry :
          generalStateTests.entrySet()) {
        if (testName == null || testName.equals(generalStateTestEntry.getKey())) {
          generalStateTestEntry
              .getValue()
              .finalStateSpecs()
              .forEach(
                  (__, specs) ->
                      traceTestSpecs(generalStateTestEntry.getKey(), specs, isLastIteration));
        }
      }
    }
  }

  private void traceTestSpecs(
      final String test,
      final List<GeneralStateTestCaseEipSpec> specs,
      final boolean isLastIteration) {
    final OperationTracer tracer = // You should have picked Mercy.
        parentCommand.showJsonResults && isLastIteration
            ? new StreamingOperationTracer(
                parentCommand.out,
                OpCodeTracerConfigBuilder.create()
                    .traceMemory(parentCommand.showMemory)
                    .traceStack(!parentCommand.hideStack)
                    .traceReturnData(parentCommand.showReturnData)
                    .traceStorage(parentCommand.showStorage)
                    .traceOpcodes(Collections.emptySet())
                    .eip3155Strict(parentCommand.eip3155strict)
                    .build())
            : OperationTracer.NO_TRACING;

    final ObjectMapper objectMapper = JsonUtils.createObjectMapper();
    for (final GeneralStateTestCaseEipSpec spec : specs) {
      if (dataIndex != null && spec.getDataIndex() != dataIndex) {
        continue;
      }
      if (gasIndex != null && spec.getGasIndex() != gasIndex) {
        continue;
      }
      if (valueIndex != null && spec.getValueIndex() != valueIndex) {
        continue;
      }
      if (forkIndex != null && !spec.getFork().equalsIgnoreCase(forkIndex)) {
        continue;
      }

      final BlockHeader blockHeader = spec.getBlockHeader();
      final Transaction transaction = spec.getTransaction(0);
      final ObjectNode summaryLine = objectMapper.createObjectNode();
      if (transaction == null) {
        if ((parentCommand.showJsonAlloc || parentCommand.showJsonResults) && isLastIteration) {
          parentCommand.out.println(
              "{\"error\":\"Transaction was invalid, trace and alloc unavailable.\"}");
        }
        summaryLine.put("test", test);
        summaryLine.put("fork", spec.getFork());
        summaryLine.put("d", spec.getDataIndex());
        summaryLine.put("g", spec.getGasIndex());
        summaryLine.put("v", spec.getValueIndex());
        summaryLine.put("pass", spec.getExpectException() != null);
        summaryLine.put("validationError", "Transaction had out-of-bounds parameters");
      } else {
        final MutableWorldState worldState = spec.getInitialWorldState().copy();
        // Several of the GeneralStateTests check if the transaction could potentially
        // consume more gas than is left for the block it's attempted to be included in.
        // This check is performed within the `BlockImporter` rather than inside the
        // `TransactionProcessor`, so these tests are skipped.
        if (transaction.getGasLimit() > blockHeader.getGasLimit() - blockHeader.getGasUsed()) {
          return;
        }

        final String forkName = fork == null ? spec.getFork() : fork;
        final ProtocolSchedule protocolSchedule =
            ReferenceTestProtocolSchedules.create(parentCommand.getEvmConfiguration())
                .getByName(forkName);
        if (protocolSchedule == null) {
          throw new UnsupportedForkException(forkName);
        }

        final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(blockHeader);
        final MainnetTransactionProcessor processor = protocolSpec.getTransactionProcessor();
        final WorldUpdater worldStateUpdater = worldState.updater();
        final Stopwatch timer = Stopwatch.createStarted();
        // EIP-4844: use excessBlobGas from block header for correct blob gas price calculation
        final BlobGas excessBlobGas = blockHeader.getExcessBlobGas().orElse(BlobGas.ZERO);
        final Wei blobGasPrice = protocolSpec.getFeeMarket().blobGasPricePerGas(excessBlobGas);
        final TransactionProcessingResult result =
            processor.processTransaction(
                worldStateUpdater,
                blockHeader,
                transaction,
                blockHeader.getCoinbase(),
                tracer,
                (__, blockNumber) ->
                    Hash.hash(Bytes.wrap(Long.toString(blockNumber).getBytes(UTF_8))),
                TransactionValidationParams.processingBlock(),
                blobGasPrice);
        timer.stop();
        // Only touch coinbase and commit state changes if the transaction was valid.
        // When transaction fails validation (e.g., insufficient balance), we should not
        // modify state at all - matching geth's behavior for consensus compatibility.
        if (!result.isInvalid()) {
          if (shouldClearEmptyAccounts(spec.getFork())) {
            final Account coinbase =
                worldStateUpdater.getOrCreate(spec.getBlockHeader().getCoinbase());
            if (coinbase != null && coinbase.isEmpty()) {
              worldStateUpdater.deleteAccount(coinbase.getAddress());
            }
            final Account sender = worldStateUpdater.getAccount(transaction.getSender());
            if (sender != null && sender.isEmpty()) {
              worldStateUpdater.deleteAccount(sender.getAddress());
            }
          }
          worldStateUpdater.commit();
          worldState.persist(blockHeader);
        }

        summaryLine.put("output", result.getOutput().toUnprefixedHexString());
        final long gasUsed = transaction.getGasLimit() - result.getGasRemaining();
        final long timeNs = timer.elapsed(TimeUnit.NANOSECONDS);
        final float mGps = gasUsed * 1000.0f / timeNs;

        if (parentCommand.eip3155strict) {
          summaryLine.put("gasUsed", StreamingOperationTracer.shortNumber(gasUsed));
        } else {
          summaryLine.put("gasUsed", gasUsed);
        }

        if (!parentCommand.noTime) {
          summaryLine.put("time", timeNs);
          summaryLine.put("Mgps", String.format("%.3f", mGps));
        }

        // Check the world state root hash.
        summaryLine.put("test", test);
        summaryLine.put("fork", spec.getFork());
        summaryLine.put("d", spec.getDataIndex());
        summaryLine.put("g", spec.getGasIndex());
        summaryLine.put("v", spec.getValueIndex());
        summaryLine.put("stateRoot", worldState.rootHash().getBytes().toHexString());
        final List<Log> logs = result.getLogs();
        final Hash actualLogsHash = Hash.hash(RLP.encode(out -> out.writeList(logs, Log::writeTo)));
        summaryLine.put("postLogsHash", actualLogsHash.getBytes().toHexString());
        summaryLine.put(
            "pass",
            spec.getExpectException() == null
                && worldState.rootHash().equals(spec.getExpectedRootHash())
                && actualLogsHash.equals(spec.getExpectedLogsHash()));
        if (result.isInvalid()) {
          summaryLine.put("validationError", result.getValidationResult().getErrorMessage());
        } else if (spec.getExpectException() != null) {
          summaryLine.put(
              "validationError",
              "Exception '" + spec.getExpectException() + "' was expected but did not occur");
        }
        if (!result.getValidationResult().isValid()) {
          summaryLine.put("error", result.getValidationResult().getErrorMessage());
        }
        if (parentCommand.showJsonAlloc && isLastIteration) {
          EvmToolCommand.dumpWorldState(worldState, parentCommand.out);
        }
      }

      if (isLastIteration) {
        parentCommand.out.println(summaryLine);
      }
    }
  }
}
