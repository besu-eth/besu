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

import static org.hyperledger.besu.evmtool.B11rSubCommand.COMMAND_ALIAS;
import static org.hyperledger.besu.evmtool.B11rSubCommand.COMMAND_NAME;

import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.referencetests.BlockchainReferenceTestCaseSpec.ReferenceTestBlockHeader;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.util.LogConfigurator;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tuweni.bytes.Bytes;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IParameterConsumer;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

/**
 * This class implements the Runnable interface and represents the B11rSubCommand. It is responsible
 * for handling the block builder subcommand in the EVM tool. It provides methods to read headers,
 * move fields, and run the command.
 */
@Command(
    name = COMMAND_NAME,
    aliases = {COMMAND_ALIAS},
    description = "Block Builder subcommand.",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class B11rSubCommand implements Runnable {
  static final String COMMAND_NAME = "block-builder";
  static final String COMMAND_ALIAS = "b11r";
  private static final Path stdoutPath = Path.of("stdout");
  private static final Path stdinPath = Path.of("stdin");

  @Option(
      names = {"--input.header"},
      paramLabel = "full path",
      description = "The block header for the block")
  private final Path header = stdinPath;

  @Option(
      names = {"--input.txs"},
      paramLabel = "full path",
      description = "The transactions to block")
  private final Path txs = stdinPath;

  @Option(
      names = {"--input.ommers"},
      paramLabel = "full path",
      description = "The ommers for the block")
  private final Path ommers = stdinPath;

  @Option(
      names = {"--input.withdrawals"},
      paramLabel = "full path",
      description = "The withdrawals for the block")
  private final Path withdrawals = stdinPath;

  @Option(
      names = {"--seal.clique"},
      paramLabel = "full path",
      description = "The clique seal/signature for the block")
  private final Path sealClique = stdinPath;

  @SuppressWarnings("UnusedVariable")
  @Option(
      names = {"--seal.ethash"},
      description = "Use Proof of Work to seal the block")
  private final Boolean sealEthash = false;

  @SuppressWarnings("UnusedVariable")
  @Option(
      names = {"--seal.ethash.mode"},
      paramLabel = "full path",
      description = "The ethash mode for the block")
  private String sealEthashMode = "noproof";

  @Option(
      names = {"--output.basedir"},
      paramLabel = "full path",
      description = "The output ")
  private final Path outDir = Path.of(".");

  @Option(
      names = {"--output.block"},
      paramLabel = "file name",
      description = "The account state after the transition")
  private final Path outBlock = Path.of("block.json");

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
          throw new CommandLine.ParameterException(
              argSpec.command().commandLine(),
              "The block-builder command does not accept any non-empty parameters");
        }
      }
    }
  }

  /** Default constructor for the B11rSubCommand class. This is required by PicoCLI. */
  @SuppressWarnings("unused")
  public B11rSubCommand() {
    // PicoCLI requires this
    parentCommand = null;
  }

  /**
   * Constructs a new B11rSubCommand with the given parent command.
   *
   * @param parentCommand the parent command of this subcommand
   */
  @SuppressWarnings("unused")
  public B11rSubCommand(final EvmToolCommand parentCommand) {
    // PicoCLI requires this too
    this.parentCommand = parentCommand;
  }

  @Override
  public void run() {
    LogConfigurator.setLevel("", "OFF");
    ObjectMapper objectMapper = JsonUtils.createObjectMapper();
    final ObjectReader b11rReader = objectMapper.reader();

    ObjectNode config;
    try {
      if (header.equals(stdinPath)
          || txs.equals(stdinPath)
          || ommers.equals(stdinPath)
          || sealClique.equals(stdinPath)
          || withdrawals.equals(stdinPath)) {
        config =
            (ObjectNode)
                b11rReader.readTree(
                    new InputStreamReader(parentCommand.in, StandardCharsets.UTF_8));
      } else {
        config = objectMapper.createObjectNode();
      }

      if (!header.equals(stdinPath)) {
        try (FileReader reader = new FileReader(header.toFile(), StandardCharsets.UTF_8)) {
          config.set("header", b11rReader.readTree(reader));
        }
      }
      if (!txs.equals(stdinPath)) {
        try (FileReader reader = new FileReader(txs.toFile(), StandardCharsets.UTF_8)) {
          config.set("txs", b11rReader.readTree(reader));
        }
      }
      if (!withdrawals.equals(stdinPath)) {
        try (FileReader reader = new FileReader(withdrawals.toFile(), StandardCharsets.UTF_8)) {
          config.set("withdrawals", b11rReader.readTree(reader));
        }
      }
      if (!ommers.equals(stdinPath)) {
        try (FileReader reader = new FileReader(ommers.toFile(), StandardCharsets.UTF_8)) {
          config.set("ommers", b11rReader.readTree(reader));
        }
      }
      if (!sealClique.equals(stdinPath)) {
        try (FileReader reader = new FileReader(sealClique.toFile(), StandardCharsets.UTF_8)) {
          config.set("clique", b11rReader.readTree(reader));
        }
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

    var testHeader = this.readHeader(config.get("header"), objectMapper);
    Bytes txsBytes = null;

    if (config.has("txs")) {
      String txsString = config.get("txs").textValue();
      if (!txsString.isEmpty()) {
        txsBytes = Bytes.fromHexString(txsString);
      }
    }

    var newHeader =
        BlockHeaderBuilder.fromHeader(testHeader)
            .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
            .buildBlockHeader();
    BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    rlpOut.startList();
    newHeader.writeTo(rlpOut);
    if (txsBytes != null && !txsBytes.isEmpty()) {
      rlpOut.writeRaw(txsBytes);
    } else {
      rlpOut.startList();
      rlpOut.endList();
    }
    // ommers
    rlpOut.startList();
    rlpOut.endList();

    // withdrawals
    // TODO - waiting on b11r spec to specify how withdrawals are added to blocks.

    rlpOut.endList();

    final ObjectNode resultObject = objectMapper.createObjectNode();
    resultObject.put("rlp", rlpOut.encoded().toHexString());
    resultObject.put("hash", newHeader.getHash().getBytes().toHexString());
    var writer = objectMapper.writerWithDefaultPrettyPrinter();
    try {
      var resultString = writer.writeValueAsString(resultObject);
      if (outBlock.equals((stdoutPath))) {
        parentCommand.out.println(resultString);
      } else {
        try (var fileOut =
            new PrintStream(new FileOutputStream(outDir.resolve(outBlock).toFile()))) {
          fileOut.println(resultString);
        }
      }
    } catch (FileNotFoundException | JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  void maybeMoveField(final ObjectNode jsonObject, final String oldField, final String newField) {
    if (jsonObject.has(oldField)) {
      jsonObject.set(newField, jsonObject.remove(oldField));
    }
  }

  private ReferenceTestBlockHeader readHeader(
      final JsonNode jsonObject, final ObjectMapper objectMapper) {
    ObjectNode objectNode = (ObjectNode) jsonObject;
    maybeMoveField(objectNode, "sha3Uncles", "uncleHash");
    maybeMoveField(objectNode, "miner", "coinbase");
    maybeMoveField(objectNode, "transactionsRoot", "transactionsTrie");
    maybeMoveField(objectNode, "receiptsRoot", "receiptTrie");
    maybeMoveField(objectNode, "logsBloom", "bloom");
    return objectMapper.convertValue(jsonObject, ReferenceTestBlockHeader.class);
  }
}
