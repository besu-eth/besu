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
package org.hyperledger.besu.cli.subcommands;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hyperledger.besu.cli.subcommands.TxParseSubCommand.COMMAND_NAME;

import org.hyperledger.besu.cli.util.VersionProvider;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import picocli.CommandLine;

/**
 * txparse sub command implementing txparse spec from
 * https://github.com/holiman/txparse/tree/main/cmd/txparse
 */
@CommandLine.Command(
    name = COMMAND_NAME,
    description = "Parse input transactions and return the sender, or an error.",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class)
public class TxParseSubCommand implements Runnable {

  /** The constant COMMAND_NAME. */
  public static final String COMMAND_NAME = "txparse";

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @CommandLine.Option(
      names = "--corpus-file",
      arity = "1..1",
      description = "file to read transaction data lines from, otherwise defaults to stdin")
  private String corpusFile = null;

  static final BigInteger chainId = new BigInteger("1", 10);

  private final PrintWriter out;

  /**
   * Instantiates a new TxParse sub command.
   *
   * @param out the PrintWriter where validation results will be reported.
   */
  public TxParseSubCommand(final PrintWriter out) {
    this.out = out;
  }

  @SuppressWarnings("StreamResourceLeak")
  Stream<String> fileStreamReader(final String filePath) {
    try {
      return Files.lines(Paths.get(filePath))
          // skip comments
          .filter(line -> !line.startsWith("#"))
          // strip out non-alphanumeric characters
          .map(line -> line.replaceAll("[^a-zA-Z0-9]", ""));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void run() {

    Stream<String> txStream;
    if (corpusFile != null) {
      txStream = fileStreamReader(corpusFile);
    } else {
      txStream = new BufferedReader(new InputStreamReader(System.in, UTF_8)).lines();
    }

    txStream.forEach(
        line -> {
          try {
            Bytes bytes = Bytes.fromHexStringLenient(line);
            dump(bytes);
          } catch (Exception ex) {
            err(ex.getMessage());
          }
        });
  }

  void dump(final Bytes tx) {
    try {
      var transaction = TransactionDecoder.decodeOpaqueBytes(tx, EncodingContext.BLOCK_BODY);

      // https://github.com/hyperledger/besu/blob/5fe49c60b30fe2954c7967e8475c3b3e9afecf35/ethereum/core/src/main/java/org/hyperledger/besu/ethereum/mainnet/MainnetTransactionValidator.java#L252
      if (transaction.getChainId().isPresent() && !transaction.getChainId().get().equals(chainId)) {
        throw new Exception("wrong chain id");
      }

      // https://github.com/hyperledger/besu/blob/5fe49c60b30fe2954c7967e8475c3b3e9afecf35/ethereum/core/src/main/java/org/hyperledger/besu/ethereum/mainnet/MainnetTransactionValidator.java#L270
      if (transaction.getS().compareTo(SignatureAlgorithmFactory.getInstance().getHalfCurveOrder())
          > 0) {
        throw new Exception("signature s out of range");
      }
      out.println(transaction.getSender());

    } catch (Exception ex) {
      err(ex.getMessage());
    }
  }

  void err(final String message) {
    out.println("err: " + message);
  }
}
