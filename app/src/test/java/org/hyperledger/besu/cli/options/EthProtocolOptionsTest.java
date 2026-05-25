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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyNoInteractions;

import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.ImmutableEthProtocolConfiguration;

import java.util.List;
import java.util.function.ToIntFunction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EthProtocolOptionsTest
    extends AbstractCLIOptionsTest<EthProtocolConfiguration, EthProtocolOptions> {

  record MaxSizeTestInput(
      String optionName, ToIntFunction<EthProtocolConfiguration> configGetter) {}

  static List<MaxSizeTestInput> parsesValidMaxSizeOptions =
      List.of(
          new MaxSizeTestInput(
              "--Xeth-max-message-size", EthProtocolConfiguration::getMaxMessageSize),
          new MaxSizeTestInput(
              "--Xeth-max-transactions-message-size",
              EthProtocolConfiguration::getMaxTransactionsMessageSize));

  @ParameterizedTest
  @FieldSource("parsesValidMaxSizeOptions")
  public void parsesValidMaxSizeOptions(final MaxSizeTestInput input) {
    final TestBesuCommand cmd = parseCommand(input.optionName, "4");

    final EthProtocolOptions options = getOptionsFromBesuCommand(cmd);
    final EthProtocolConfiguration config = options.toDomainObject();
    assertThat(input.configGetter.applyAsInt(config)).isEqualTo(4);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @ParameterizedTest
  @FieldSource("parsesValidMaxSizeOptions")
  public void parsesInvalidMaxMessageSizeOptionsShouldFail(final MaxSizeTestInput input) {
    parseCommand(input.optionName, "-4");
    verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Invalid value for option '%s': cannot convert '-4' to PositiveNumber"
                .formatted(input.optionName));
  }

  @Test
  public void parsesValidEwpMaxGetHeadersOptions() {

    final TestBesuCommand cmd = parseCommand("--Xewp-max-get-headers", "13");

    final EthProtocolOptions options = getOptionsFromBesuCommand(cmd);
    final EthProtocolConfiguration config = options.toDomainObject();
    assertThat(config.getMaxGetBlockHeaders()).isEqualTo(13);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void parsesInvalidEwpMaxGetHeadersOptionsShouldFail() {
    parseCommand("--Xewp-max-get-headers", "-13");
    verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Invalid value for option '--Xewp-max-get-headers': cannot convert '-13' to PositiveNumber");
  }

  @Test
  public void parsesValidEwpMaxGetBodiesOptions() {
    final TestBesuCommand cmd = parseCommand("--Xewp-max-get-bodies", "14");

    final EthProtocolOptions options = getOptionsFromBesuCommand(cmd);
    final EthProtocolConfiguration config = options.toDomainObject();
    assertThat(config.getMaxGetBlockBodies()).isEqualTo(14);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void parsesInvalidEwpMaxGetBodiesOptionsShouldFail() {
    parseCommand("--Xewp-max-get-bodies", "-14");
    verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Invalid value for option '--Xewp-max-get-bodies': cannot convert '-14' to PositiveNumber");
  }

  @Test
  public void parsesValidEwpMaxGetReceiptsOptions() {
    final TestBesuCommand cmd = parseCommand("--Xewp-max-get-receipts", "15");

    final EthProtocolOptions options = getOptionsFromBesuCommand(cmd);
    final EthProtocolConfiguration config = options.toDomainObject();
    assertThat(config.getMaxGetReceipts()).isEqualTo(15);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void parsesInvalidEwpMaxGetReceiptsOptionsShouldFail() {
    parseCommand("--Xewp-max-get-receipts", "-15");

    verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains(
            "Invalid value for option '--Xewp-max-get-receipts': cannot convert '-15' to PositiveNumber");
  }

  @Test
  public void parsesValidEthMaxProtocol() {
    final TestBesuCommand cmd = parseCommand("--Xeth-capability-max", "66");

    final EthProtocolOptions options = getOptionsFromBesuCommand(cmd);
    final EthProtocolConfiguration config = options.toDomainObject();
    assertThat(config.getMaxEthCapability()).isEqualTo(66);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void parsesValidEthMinProtocol() {
    final TestBesuCommand cmd = parseCommand("--Xeth-capability-min", "66");

    final EthProtocolOptions options = getOptionsFromBesuCommand(cmd);
    final EthProtocolConfiguration config = options.toDomainObject();
    assertThat(config.getMinEthCapability()).isEqualTo(66);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Override
  protected EthProtocolConfiguration createDefaultDomainObject() {
    return ImmutableEthProtocolConfiguration.builder().build();
  }

  @Override
  protected EthProtocolConfiguration createCustomizedDomainObject() {
    return ImmutableEthProtocolConfiguration.builder()
        .maxMessageSize(EthProtocolConfiguration.DEFAULT_MAX_MESSAGE_SIZE * 2)
        .maxTransactionsMessageSize(
            EthProtocolConfiguration.DEFAULT_MAX_TRANSACTIONS_MESSAGE_SIZE * 2)
        .maxGetBlockHeaders(EthProtocolConfiguration.DEFAULT_MAX_GET_BLOCK_HEADERS + 2)
        .maxGetBlockBodies(EthProtocolConfiguration.DEFAULT_MAX_GET_BLOCK_BODIES + 2)
        .maxGetReceipts(EthProtocolConfiguration.DEFAULT_MAX_GET_RECEIPTS + 2)
        .maxGetPooledTransactions(EthProtocolConfiguration.DEFAULT_MAX_GET_POOLED_TRANSACTIONS + 2)
        .maxEthCapability(EthProtocolConfiguration.DEFAULT_MAX_CAPABILITY)
        .minEthCapability(EthProtocolConfiguration.DEFAULT_MIN_CAPABILITY)
        .build();
  }

  @Override
  protected EthProtocolOptions optionsFromDomainObject(
      final EthProtocolConfiguration domainObject) {
    return EthProtocolOptions.fromConfig(domainObject);
  }

  @Override
  protected EthProtocolOptions getOptionsFromBesuCommand(final TestBesuCommand besuCommand) {
    return besuCommand.getEthProtocolOptions();
  }
}
