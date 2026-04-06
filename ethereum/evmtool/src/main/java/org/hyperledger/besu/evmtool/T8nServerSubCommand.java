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

import static org.hyperledger.besu.evmtool.T8nExecutor.extractTransactions;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestEnv;
import org.hyperledger.besu.ethereum.referencetests.ReferenceTestWorldState;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.tracing.OpCodeTracerConfigBuilder;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.tracing.StreamingOperationTracer;
import org.hyperledger.besu.evmtool.T8nExecutor.RejectedTransaction;
import org.hyperledger.besu.util.LogConfigurator;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import picocli.CommandLine;
import picocli.CommandLine.ParentCommand;

/**
 * The T8nServerSubCommand class is responsible for running an Ethereum State Test server. It reads
 * the initial state, transactions, and environment from input files or stdin, executes the
 * transactions in the Ethereum Virtual Machine (EVM), and writes the final state, transaction
 * results, and traces to output files or stdout.
 *
 * <p>The class uses the Vert.x library for handling HTTP requests and the picocli library for
 * command line argument parsing. It includes options for specifying the host and port to bind to,
 * and the base directory for output.
 *
 * <p>The class also includes a TracerManager for managing OperationTracer instances, which are used
 * to trace EVM operations when the --json flag is specified.
 */
@SuppressWarnings("java:S106") // using standard output is the point of this class
@CommandLine.Command(
    name = "t8n-server",
    description = "Run Ethereum State Test server",
    versionProvider = VersionProvider.class)
public class T8nServerSubCommand implements Runnable {

  @CommandLine.Option(
      names = {"--host"},
      description = "Host to bind to")
  private String host = "localhost";

  @CommandLine.Option(
      names = {"--port"},
      description = "Port to bind to")
  private int port = 3000;

  @CommandLine.Option(
      names = {"--output.basedir"},
      paramLabel = "full path",
      description = "The output ")
  private final Path outDir = Path.of(".");

  @ParentCommand private final EvmToolCommand parentCommand;

  /**
   * Default constructor for the T8nServerSubCommand class. This constructor is required by PicoCLI
   * and assigns null to parentCommand.
   */
  @SuppressWarnings("unused")
  public T8nServerSubCommand() {
    // PicoCLI requires this
    this(null);
  }

  T8nServerSubCommand(final EvmToolCommand parentCommand) {
    this.parentCommand = parentCommand;
  }

  @Override
  public void run() {
    LogConfigurator.setLevel("", "OFF");
    Vertx.vertx()
        .createHttpServer(
            new HttpServerOptions()
                .setHost(host)
                .setPort(port)
                .setHandle100ContinueAutomatically(true)
                .setCompressionSupported(true))
        .requestHandler(req -> req.bodyHandler(body -> handle(req, body)))
        .listen()
        .onSuccess(
            server -> System.out.println("Transition server listening on " + server.actualPort()))
        .onFailure(
            err -> System.err.println("Failed to start transition server: " + err.getMessage()));
  }

  void handle(final HttpServerRequest req, final Buffer body) {
    ObjectMapper objectMapper = JsonUtils.createObjectMapper();
    final ObjectReader t8nReader = objectMapper.reader();
    try {
      var t8nRequest = t8nReader.readTree(body.toString());
      JsonNode state = t8nRequest.get("state");
      JsonNode input = t8nRequest.get("input");

      if (state != null && input != null) {
        handleT8nRequest(req, objectMapper, state, input);
      } else {
        sendHelp(req, objectMapper);
      }
      req.response().send();
    } catch (JsonProcessingException e) {
      req.response().setStatusCode(500).end(e.getMessage());
    }
  }

  void handleT8nRequest(
      final HttpServerRequest req,
      final ObjectMapper objectMapper,
      final JsonNode state,
      final JsonNode input) {
    try {
      String fork = state.get("fork").asText();
      Long chainId = Long.valueOf(state.get("chainid").asText());
      String reward = state.get("reward").asText();

      ReferenceTestEnv referenceTestEnv =
          objectMapper.convertValue(input.get("env"), ReferenceTestEnv.class);
      Map<String, ReferenceTestWorldState.AccountMock> accounts =
          objectMapper.convertValue(input.get("alloc"), new TypeReference<>() {});

      final T8nExecutor.T8nResult result;
      try (ReferenceTestWorldState initialWorldState =
          ReferenceTestWorldState.create(accounts, EvmConfiguration.DEFAULT)) {
        initialWorldState.persist(null);
        List<Transaction> transactions = new ArrayList<>();
        List<RejectedTransaction> rejections = new ArrayList<>();
        JsonNode txs = input.get("txs");
        if (txs != null) {
          if (txs instanceof ArrayNode txsArray) {
            extractTransactions(
                new PrintWriter(System.err, true, StandardCharsets.UTF_8),
                txsArray.elements(),
                transactions,
                rejections);
          } else if (txs instanceof TextNode txt) {
            transactions =
                extractTransactions(
                    new PrintWriter(System.err, true, StandardCharsets.UTF_8),
                    List.<JsonNode>of(txt).iterator(),
                    transactions,
                    rejections);
          }
        }

        T8nExecutor.TracerManager tracerManager;
        if (parentCommand.showJsonResults) {
          tracerManager =
              new T8nExecutor.TracerManager() {
                private final Map<OperationTracer, FileOutputStream> outputStreams =
                    new HashMap<>();

                @Override
                public OperationTracer getManagedTracer(final int txIndex, final Hash txHash)
                    throws Exception {
                  outDir.toFile().mkdirs();
                  var traceDest =
                      new FileOutputStream(
                          outDir
                              .resolve(
                                  String.format(
                                      "trace-%d-%s.jsonl",
                                      txIndex, txHash.getBytes().toHexString()))
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

        result =
            T8nExecutor.runTest(
                chainId,
                fork,
                reward,
                objectMapper,
                referenceTestEnv,
                initialWorldState,
                transactions,
                rejections,
                tracerManager,
                parentCommand.getEvmConfiguration());
      }

      ObjectNode outputObject = objectMapper.createObjectNode();
      outputObject.set("alloc", result.allocObject());
      outputObject.set("body", result.bodyBytes());
      outputObject.set("result", result.resultObject());

      String response =
          objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(outputObject);
      req.response().setChunked(true);
      req.response().putHeader("Content-Type", "application/json").send(response);
    } catch (Exception t) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8);
      t.printStackTrace(ps);
      ObjectNode json = objectMapper.createObjectNode();
      json.put("error", t.getMessage());
      json.put("stacktrace", baos.toString(StandardCharsets.UTF_8).replaceAll("\\s", " "));

      t.printStackTrace(System.out);

      req.response().setStatusCode(500).end(json.toString());
    }
  }

  private void sendHelp(final HttpServerRequest req, final ObjectMapper objectMapper) {
    ObjectNode outputObject = objectMapper.createObjectNode();
    outputObject.set("version", TextNode.valueOf(new VersionProvider().getVersion()[0]));
    ArrayNode forks = objectMapper.createArrayNode();
    outputObject.set("forks", forks);
    for (var fork : EvmSpecVersion.values()) {
      forks.add(TextNode.valueOf(fork.getName()));
    }
    outputObject.set("error", TextNode.valueOf("Both 'state' and 'input' fields must be set"));

    try {
      req.response()
          .putHeader("Content-Type", "application/json")
          .end(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(outputObject));

    } catch (JsonProcessingException e) {
      req.response().setStatusCode(500).end(e.getMessage());
    }
  }
}
