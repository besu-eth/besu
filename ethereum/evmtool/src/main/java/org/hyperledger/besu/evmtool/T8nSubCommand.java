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

import static org.hyperledger.besu.evmtool.T8nSubCommand.COMMAND_ALIAS;
import static org.hyperledger.besu.evmtool.T8nSubCommand.COMMAND_NAME;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestEnv;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestWorldState;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.tracing.StreamingOperationTracer;
import org.hyperledger.besu.evmtool.T8nExecutor.RejectedTransaction;
import org.hyperledger.besu.util.LogConfigurator;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import picocli.CommandLine.Command;
import picocli.CommandLine.IParameterConsumer;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

/**
 * The T8nSubCommand class is responsible for executing an Ethereum State Test. It reads the initial
 * state, transactions, and environment from input files or stdin, executes the transactions in the
 * Ethereum Virtual Machine (EVM), and writes the final state, transaction results, and traces to
 * output files or stdout.
 *
 * <p>The class uses the picocli library for command line argument parsing and includes options for
 * specifying the input and output files, the fork to run the transition against, the chain ID, and
 * the block reward.
 *
 * <p>The class also includes a TracerManager for managing OperationTracer instances, which are used
 * to trace EVM operations when the --json flag is specified.
 */
@Command(
    name = COMMAND_NAME,
    aliases = COMMAND_ALIAS,
    description = "Execute an Ethereum State Test.",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class T8nSubCommand implements Runnable {

  static final String COMMAND_NAME = "transition";
  static final String COMMAND_ALIAS = "t8n";
  private static final Path stdoutPath = Path.of("stdout");
  private static final Path stdinPath = Path.of("stdin");

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"})
  @Option(
      names = {"--state.fork"},
      paramLabel = "fork name",
      description = "The fork to run the transition against")
  private String fork = null;

  @Option(
      names = {"--input.env"},
      paramLabel = "full path",
      description = "The block environment for the transition")
  private final Path env = stdinPath;

  @Option(
      names = {"--input.alloc"},
      paramLabel = "full path",
      description = "The account state for the transition")
  private final Path alloc = stdinPath;

  @Option(
      names = {"--input.txs"},
      paramLabel = "full path",
      description = "The transactions to transition")
  private final Path txs = stdinPath;

  @Option(
      names = {"--output.basedir"},
      paramLabel = "full path",
      description = "The output ")
  private final Path outDir = Path.of(".");

  @Option(
      names = {"--output.alloc"},
      paramLabel = "file name",
      description = "The account state after the transition")
  private final Path outAlloc = Path.of("alloc.json");

  @Option(
      names = {"--output.result"},
      paramLabel = "file name",
      description = "The summary of the transition")
  private final Path outResult = Path.of("result.json");

  @Option(
      names = {"--output.body"},
      paramLabel = "file name",
      description = "RLP of transactions considered")
  private final Path outBody = Path.of("txs.rlp");

  @Option(
      names = {"--state.chainid"},
      paramLabel = "chain ID",
      description = "The chain Id to use")
  private final Long chainId = 1L;

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"})
  @Option(
      names = {"--state.reward"},
      paramLabel = "block mining reward",
      description = "The block reward to use in block tess")
  private String rewardString = null;

  @ParentCommand private final EvmToolCommand parentCommand;

  @Parameters(parameterConsumer = OnlyEmptyParams.class)
  @SuppressWarnings("UnusedVariable")
  private final List<String> parameters = new ArrayList<>();

  static class OnlyEmptyParams implements IParameterConsumer {
    @Override
    public void consumeParameters(
        final Stack<String> args, final ArgSpec argSpec, final CommandSpec commandSpec) {
      while (!args.isEmpty()) {
        if (!args.pop().isEmpty()) {
          throw new ParameterException(
              argSpec.command().commandLine(),
              "The transition command does not accept any non-empty parameters");
        }
      }
    }
  }

  /**
   * Default constructor for the T8nSubCommand class. This constructor is required by PicoCLI and
   * assigns parent command to 'null'.
   */
  @SuppressWarnings("unused")
  public T8nSubCommand() {
    // PicoCLI requires this
    parentCommand = null;
  }

  /**
   * Constructor for the T8nSubCommand class with a parent command. This constructor is required by
   * PicoCLI.
   *
   * @param parentCommand The parent command for this sub command.
   */
  @SuppressWarnings("unused")
  public T8nSubCommand(final EvmToolCommand parentCommand) {
    // PicoCLI requires this too
    this.parentCommand = parentCommand;
  }

  @Override
  public void run() {
    LogConfigurator.setLevel("", "OFF");
    final ObjectMapper objectMapper = JsonUtils.createObjectMapper();
    final ObjectReader t8nReader = objectMapper.reader();

    ReferenceTestWorldState initialWorldState;
    ReferenceTestEnv referenceTestEnv;
    List<Transaction> transactions = new ArrayList<>();
    List<RejectedTransaction> rejections = new ArrayList<>();
    try {
      ObjectNode config;
      if (env.equals(stdinPath) || alloc.equals(stdinPath) || txs.equals(stdinPath)) {
        try (InputStreamReader reader =
            new InputStreamReader(parentCommand.in, StandardCharsets.UTF_8)) {
          config = (ObjectNode) t8nReader.readTree(reader);
        }
      } else {
        config = objectMapper.createObjectNode();
      }

      if (!env.equals(stdinPath)) {
        try (FileReader reader = new FileReader(env.toFile(), StandardCharsets.UTF_8)) {
          config.set("env", t8nReader.readTree(reader));
        }
      }
      if (!alloc.equals(stdinPath)) {
        try (FileReader reader = new FileReader(alloc.toFile(), StandardCharsets.UTF_8)) {
          config.set("alloc", t8nReader.readTree(reader));
        }
      }
      if (!txs.equals(stdinPath)) {
        try (FileReader reader = new FileReader(txs.toFile(), StandardCharsets.UTF_8)) {
          config.set("txs", t8nReader.readTree(reader));
        }
      }

      referenceTestEnv = objectMapper.convertValue(config.get("env"), ReferenceTestEnv.class);
      Map<String, ReferenceTestWorldState.AccountMock> accounts =
          objectMapper.convertValue(config.get("alloc"), new TypeReference<>() {});
      initialWorldState = ReferenceTestWorldState.create(accounts, EvmConfiguration.DEFAULT);
      initialWorldState.persist(null);
      var node = config.get("txs");
      Iterator<JsonNode> it;
      if (node.isArray()) {
        it = config.get("txs").elements();
      } else if (node == null || node.isNull()) {
        it = Collections.emptyIterator();
      } else {
        it = List.of(node).iterator();
      }

      T8nExecutor.extractTransactions(parentCommand.out, it, transactions, rejections);
      if (!outDir.toString().isBlank()) {
        outDir.toFile().mkdirs();
      }
    } catch (final JsonProcessingException jpe) {
      parentCommand.out.println("File content error: " + jpe);
      jpe.printStackTrace();
      return;
    } catch (final IOException e) {
      System.err.println("Unable to read state file");
      e.printStackTrace(System.err);
      return;
    }

    T8nExecutor.TracerManager tracerManager;
    if (parentCommand.showJsonResults) {
      tracerManager =
          new T8nExecutor.TracerManager() {
            private final Map<OperationTracer, FileOutputStream> outputStreams = new HashMap<>();

            @Override
            public OperationTracer getManagedTracer(final int txIndex, final Hash txHash)
                throws Exception {
              var traceDest =
                  new FileOutputStream(
                      outDir
                          .resolve(
                              String.format(
                                  "trace-%d-%s.jsonl", txIndex, txHash.getBytes().toHexString()))
                          .toFile());

              var jsonTracer =
                  new StreamingOperationTracer(
                      new PrintStream(traceDest),
                      OpCodeTracerConfigBuilder.create()
                          .traceMemory(parentCommand.showMemory)
                          .traceStack(!parentCommand.hideStack)
                          .traceReturnData(parentCommand.showReturnData)
                          .traceStorage(parentCommand.showStorage)
                          .traceOpcodes(Collections.emptySet())
                          .eip3155Strict(parentCommand.eip3155strict)
                          .build());
              outputStreams.put(jsonTracer, traceDest);
              return jsonTracer;
            }

            @Override
            public void disposeTracer(final OperationTracer tracer) throws IOException {
              if (outputStreams.containsKey(tracer)) {
                outputStreams.remove(tracer).close();
              }
            }
          };
    } else {
      tracerManager =
          new T8nExecutor.TracerManager() {
            @Override
            public OperationTracer getManagedTracer(final int txIndex, final Hash txHash) {
              return OperationTracer.NO_TRACING;
            }

            @Override
            public void disposeTracer(final OperationTracer tracer) {
              // single-test mode doesn't need to track tracers
            }
          };
    }
    final T8nExecutor.T8nResult result =
        T8nExecutor.runTest(
            chainId,
            fork,
            rewardString,
            objectMapper,
            referenceTestEnv,
            initialWorldState,
            transactions,
            rejections,
            tracerManager,
            parentCommand.getEvmConfiguration());

    try {
      ObjectWriter writer = objectMapper.writerWithDefaultPrettyPrinter();
      ObjectNode outputObject = objectMapper.createObjectNode();

      if (outAlloc.equals(stdoutPath)) {
        outputObject.set("alloc", result.allocObject());
      } else {
        try (PrintStream fileOut =
            new PrintStream(new FileOutputStream(outDir.resolve(outAlloc).toFile()))) {
          fileOut.println(writer.writeValueAsString(result.allocObject()));
        }
      }

      if (outBody.equals((stdoutPath))) {
        outputObject.set("body", result.bodyBytes());
      } else {
        try (PrintStream fileOut =
            new PrintStream(new FileOutputStream(outDir.resolve(outBody).toFile()))) {
          fileOut.print(result.bodyBytes().textValue());
        }
      }

      if (outResult.equals(stdoutPath)) {
        outputObject.set("result", result.resultObject());
      } else {
        try (PrintStream fileOut =
            new PrintStream(new FileOutputStream(outDir.resolve(outResult).toFile()))) {
          fileOut.println(writer.writeValueAsString(result.resultObject()));
        }
      }

      if (!outputObject.isEmpty()) {
        parentCommand.out.println(writer.writeValueAsString(outputObject));
      }
    } catch (IOException ioe) {
      System.err.println("Could not write results");
      ioe.printStackTrace(System.err);
    }
  }
}
