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
package org.hyperledger.besu.cli.options;

import org.hyperledger.besu.cli.util.CommandLineUtils;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.ImmutableEthProtocolConfiguration;
import org.hyperledger.besu.util.number.PositiveNumber;

import java.util.List;

import picocli.CommandLine;

/** The Eth protocol CLI options. */
public class EthProtocolOptions implements CLIOptions<EthProtocolConfiguration> {
  private static final String MAX_MESSAGE_SIZE_FLAG = "--Xeth-max-message-size";
  private static final String MAX_TRANSACTIONS_MESSAGE_SIZE_FLAG =
      "--Xeth-max-transactions-message-size";
  private static final String MAX_GET_HEADERS_FLAG = "--Xewp-max-get-headers";
  private static final String MAX_GET_BODIES_FLAG = "--Xewp-max-get-bodies";
  private static final String MAX_GET_RECEIPTS_FLAG = "--Xewp-max-get-receipts";
  private static final String MAX_GET_POOLED_TRANSACTIONS = "--Xewp-max-get-pooled-transactions";
  private static final String MAX_CAPABILITY = "--Xeth-capability-max";
  private static final String MIN_CAPABILITY = "--Xeth-capability-min";

  @CommandLine.Option(
      hidden = true,
      names = {MAX_MESSAGE_SIZE_FLAG},
      paramLabel = "<INTEGER>",
      description =
          "Maximum message size (in bytes) for Ethereum Wire Protocol messages. (default: ${DEFAULT-VALUE})")
  private PositiveNumber maxMessageSize =
      PositiveNumber.fromInt(EthProtocolConfiguration.DEFAULT_MAX_MESSAGE_SIZE);

  @CommandLine.Option(
      hidden = true,
      names = {MAX_TRANSACTIONS_MESSAGE_SIZE_FLAG},
      paramLabel = "<INTEGER>",
      description =
          "Maximum message size (in bytes) for P2P Transactions message. (default: ${DEFAULT-VALUE})")
  private PositiveNumber maxTransactionsMessageSize =
      PositiveNumber.fromInt(EthProtocolConfiguration.DEFAULT_MAX_TRANSACTIONS_MESSAGE_SIZE);

  @CommandLine.Option(
      hidden = true,
      names = {MAX_GET_HEADERS_FLAG},
      paramLabel = "<INTEGER>",
      description =
          "Maximum request limit for Ethereum Wire Protocol GET_BLOCK_HEADERS. (default: ${DEFAULT-VALUE})")
  private PositiveNumber maxGetBlockHeaders =
      PositiveNumber.fromInt(EthProtocolConfiguration.DEFAULT_MAX_GET_BLOCK_HEADERS);

  @CommandLine.Option(
      hidden = true,
      names = {MAX_GET_BODIES_FLAG},
      paramLabel = "<INTEGER>",
      description =
          "Maximum request limit for Ethereum Wire Protocol GET_BLOCK_BODIES. (default: ${DEFAULT-VALUE})")
  private PositiveNumber maxGetBlockBodies =
      PositiveNumber.fromInt(EthProtocolConfiguration.DEFAULT_MAX_GET_BLOCK_BODIES);

  @CommandLine.Option(
      hidden = true,
      names = {MAX_GET_RECEIPTS_FLAG},
      paramLabel = "<INTEGER>",
      description =
          "Maximum request limit for Ethereum Wire Protocol GET_RECEIPTS. (default: ${DEFAULT-VALUE})")
  private PositiveNumber maxGetReceipts =
      PositiveNumber.fromInt(EthProtocolConfiguration.DEFAULT_MAX_GET_RECEIPTS);

  @CommandLine.Option(
      hidden = true,
      names = {MAX_GET_POOLED_TRANSACTIONS},
      paramLabel = "<INTEGER>",
      description =
          "Maximum request limit for Ethereum Wire Protocol GET_POOLED_TRANSACTIONS. (default: ${DEFAULT-VALUE})")
  private PositiveNumber maxGetPooledTransactions =
      PositiveNumber.fromInt(EthProtocolConfiguration.DEFAULT_MAX_GET_POOLED_TRANSACTIONS);

  @CommandLine.Option(
      hidden = true,
      names = {MAX_CAPABILITY},
      paramLabel = "<INTEGER>",
      description = "Max protocol version to support")
  private int maxEthCapability = EthProtocolConfiguration.DEFAULT_MAX_CAPABILITY;

  @CommandLine.Option(
      hidden = true,
      names = {MIN_CAPABILITY},
      paramLabel = "<INTEGER>",
      description = "Min protocol version to support")
  private int minEthCapability = EthProtocolConfiguration.DEFAULT_MIN_CAPABILITY;

  private EthProtocolOptions() {}

  /**
   * Create eth protocol options.
   *
   * @return the eth protocol options
   */
  public static EthProtocolOptions create() {
    return new EthProtocolOptions();
  }

  /**
   * From config eth protocol options.
   *
   * @param config the config
   * @return the eth protocol options
   */
  public static EthProtocolOptions fromConfig(final EthProtocolConfiguration config) {
    final EthProtocolOptions options = create();
    options.maxMessageSize = PositiveNumber.fromInt(config.getMaxMessageSize());
    options.maxTransactionsMessageSize =
        PositiveNumber.fromInt(config.getMaxTransactionsMessageSize());
    options.maxGetBlockHeaders = PositiveNumber.fromInt(config.getMaxGetBlockHeaders());
    options.maxGetBlockBodies = PositiveNumber.fromInt(config.getMaxGetBlockBodies());
    options.maxGetReceipts = PositiveNumber.fromInt(config.getMaxGetReceipts());
    options.maxGetPooledTransactions = PositiveNumber.fromInt(config.getMaxGetPooledTransactions());
    options.maxEthCapability = config.getMaxEthCapability();
    options.minEthCapability = config.getMinEthCapability();
    return options;
  }

  @Override
  public EthProtocolConfiguration toDomainObject() {
    return ImmutableEthProtocolConfiguration.builder()
        .maxMessageSize(maxMessageSize.getValue())
        .maxTransactionsMessageSize(maxTransactionsMessageSize.getValue())
        .maxGetBlockHeaders(maxGetBlockHeaders.getValue())
        .maxGetBlockBodies(maxGetBlockBodies.getValue())
        .maxGetReceipts(maxGetReceipts.getValue())
        .maxGetPooledTransactions(maxGetPooledTransactions.getValue())
        .maxEthCapability(maxEthCapability)
        .minEthCapability(minEthCapability)
        .build();
  }

  @Override
  public List<String> getCLIOptions() {
    return CommandLineUtils.getCLIOptions(this, new EthProtocolOptions());
  }
}
