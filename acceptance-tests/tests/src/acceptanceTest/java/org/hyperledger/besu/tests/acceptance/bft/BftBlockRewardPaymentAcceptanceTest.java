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
package org.hyperledger.besu.tests.acceptance.bft;

import static java.util.Collections.singletonList;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.blockchain.Amount;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class BftBlockRewardPaymentAcceptanceTest extends ParameterizedBftTestBase {

  private static final Amount BLOCK_REWARD = Amount.wei(new BigInteger("5000000000000000000", 10));

  @ParameterizedTest(name = "{0} bft node factory type")
  @MethodSource("factoryFunctions")
  public void validatorsArePaidBlockReward(
      final String testName, final BftAcceptanceTestParameterization nodeFactory) throws Exception {
    setUp(testName, nodeFactory);
    final String[] validators = {"validator"};
    final BesuNode validator = nodeFactory.createNodeWithValidators(besu, "validator", validators);
    final BesuNode nonValidator =
        nodeFactory.createNodeWithValidators(besu, "nonValidator", validators);
    cluster.start(validator, nonValidator);
    final Account validator1Account = Account.create(ethTransactions, validator.getAddress());

    final int blockRewardEth = 5;
    final int blockToCheck = 2;

    cluster.verify(blockchain.minimumHeight(blockToCheck));
    cluster.verify(validator1Account.balanceAtBlockEquals(Amount.ether(0), BigInteger.ZERO));
    cluster.verify(
        validator1Account.balanceAtBlockEquals(
            Amount.ether(blockRewardEth * blockToCheck), BigInteger.valueOf(blockToCheck)));
  }

  @ParameterizedTest(name = "{0} bft node factory type")
  @MethodSource("factoryFunctions")
  public void payBlockRewardToConfiguredNode(
      final String testName, final BftAcceptanceTestParameterization nodeFactory) throws Exception {
    setUp(testName, nodeFactory);
    final String[] validators = {"validator1"};
    final BesuNode validator1 =
        nodeFactory.createNodeWithValidators(besu, "validator1", validators);
    final Optional<String> initialConfig =
        validator1.getGenesisConfigProvider().create(singletonList(validator1));
    if (initialConfig.isEmpty()) {
      throw new RuntimeException("Unable to generate genesis config.");
    }

    final String miningBeneficiaryAddress = "0x1234567890123456789012345678901234567890";
    final Account miningBeneficiaryAccount =
        Account.create(ethTransactions, Address.fromHexString(miningBeneficiaryAddress));

    final String bftOptions = formatKeyValues("miningbeneficiary", miningBeneficiaryAddress);
    final String configWithMiningBeneficiary = configureBftOptions(initialConfig.get(), bftOptions);
    validator1.setGenesisConfig(configWithMiningBeneficiary);

    cluster.start(validator1);
    final int blockRewardEth = 5;
    final int blockToCheck = 2;

    cluster.verify(blockchain.minimumHeight(blockToCheck));
    cluster.verify(miningBeneficiaryAccount.balanceAtBlockEquals(Amount.ether(0), BigInteger.ZERO));
    cluster.verify(
        miningBeneficiaryAccount.balanceAtBlockEquals(
            Amount.ether(blockRewardEth * blockToCheck), BigInteger.valueOf(blockToCheck)));
  }

  @ParameterizedTest(name = "{0} bft node factory type")
  @MethodSource("factoryFunctions")
  public void payBlockRewardAccordingToTransitions_defaultInitialMiningBeneficiary(
      final String testName, final BftAcceptanceTestParameterization nodeFactory) throws Exception {
    setUp(testName, nodeFactory);
    final List<Address> addresses = generateAddresses(2);
    final Map<Long, Optional<Address>> transitions =
        Map.of(
            2L, Optional.of(addresses.get(0)),
            3L, Optional.of(addresses.get(1)),
            6L, Optional.empty(),
            8L, Optional.of(addresses.get(0)));
    testMiningBeneficiaryTransitions(Optional.empty(), transitions);
  }

  @ParameterizedTest(name = "{0} bft node factory type")
  @MethodSource("factoryFunctions")
  public void payBlockRewardAccordingToTransitions_customInitialMiningBeneficiary(
      final String testName, final BftAcceptanceTestParameterization nodeFactory) throws Exception {
    setUp(testName, nodeFactory);
    final List<Address> addresses = generateAddresses(4);
    final Map<Long, Optional<Address>> transitions =
        Map.of(
            1L, Optional.of(addresses.get(1)),
            2L, Optional.of(addresses.get(2)),
            3L, Optional.of(addresses.get(3)),
            4L, Optional.empty());

    testMiningBeneficiaryTransitions(Optional.of(addresses.get(0)), transitions);
  }

  private List<Address> generateAddresses(final int count) {
    final List<Address> addresses = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      addresses.add(Address.fromHexString(Integer.toString(i + 1, 16)));
    }

    return addresses;
  }

  private void testMiningBeneficiaryTransitions(
      final Optional<Address> initialMiningBeneficiary,
      final Map<Long, Optional<Address>> miningBeneficiaryTransitions)
      throws Exception {
    final String[] validators = {"validator1"};
    final BesuNode validator1 =
        nodeFactory.createNodeWithValidators(besu, "validator1", validators);
    final Optional<String> initialConfig =
        validator1.getGenesisConfigProvider().create(singletonList(validator1));
    if (initialConfig.isEmpty()) {
      throw new RuntimeException("Unable to generate genesis config.");
    }

    final String miningBeneficiary =
        initialMiningBeneficiary.map(addr -> addr.getBytes().toHexString()).orElse("");
    final String bftOptions = formatKeyValues("miningbeneficiary", miningBeneficiary);
    final String configWithMiningBeneficiary =
        configureBftOptions(initialConfig.get(), bftOptions, miningBeneficiaryTransitions);
    validator1.setGenesisConfig(configWithMiningBeneficiary);

    cluster.start(validator1);

    // Check that expected address receive block reward at each block
    final NavigableMap<Long, Optional<Address>> orderedTransitions =
        new TreeMap<>(miningBeneficiaryTransitions);
    final long lastConfiguredBlock = orderedTransitions.lastKey();
    final Map<Address, Amount> accountBalances = new HashMap<>();
    for (long i = 1; i < lastConfiguredBlock + 2; i++) {
      final Address beneficiaryAddress =
          Optional.ofNullable(orderedTransitions.floorEntry(i))
              .flatMap(Entry::getValue)
              .orElse(validator1.getAddress());
      final Account beneficiary = Account.create(ethTransactions, beneficiaryAddress);

      final Amount currentBalance =
          accountBalances.computeIfAbsent(beneficiaryAddress, __ -> Amount.wei(BigInteger.ZERO));
      cluster.verify(blockchain.minimumHeight(i));
      cluster.verify(beneficiary.balanceAtBlockEquals(currentBalance, BigInteger.valueOf(i - 1)));
      final Amount newBalance = currentBalance.add(BLOCK_REWARD);
      cluster.verify(beneficiary.balanceAtBlockEquals(newBalance, BigInteger.valueOf(i)));
      accountBalances.put(beneficiaryAddress, newBalance);
    }
  }

  private String formatKeyValues(final Object... keyOrValue) {
    if (keyOrValue.length % 2 == 1) {
      // An odd number of strings cannot form a set of key-value pairs
      throw new IllegalArgumentException("Must supply key-value pairs");
    }
    final StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < keyOrValue.length; i += 2) {
      if (i > 0) {
        stringBuilder.append(", ");
      }
      final String key = keyOrValue[i].toString();
      final Object value = keyOrValue[i + 1];
      final String valueStr = value instanceof String ? quote(value) : value.toString();
      stringBuilder.append(String.format("\n%s: %s", quote(key), valueStr));
    }
    return stringBuilder.toString();
  }

  private String quote(final Object value) {
    return '"' + value.toString() + '"';
  }

  private String configureBftOptions(final String originalOptions, final String bftOptions) {
    return configureBftOptions(originalOptions, bftOptions, Collections.emptyMap());
  }

  private String configureBftOptions(
      final String originalOptions,
      final String bftOptions,
      final Map<Long, Optional<Address>> transitions) {
    final StringBuilder stringBuilder =
        new StringBuilder()
            .append(formatBftTransitionsOptions(transitions))
            .append(",\n")
            .append(quote(bftType))
            .append(": {")
            .append(bftOptions)
            .append(",");
    return originalOptions.replace(quote(bftType) + ": {", stringBuilder.toString());
  }

  private String formatBftTransitionsOptions(final Map<Long, Optional<Address>> transitions) {
    final StringBuilder stringBuilder = new StringBuilder();

    stringBuilder.append(quote("transitions"));
    stringBuilder.append(": {\n");
    stringBuilder.append(quote(bftType));
    stringBuilder.append(": [");

    boolean isFirst = true;
    for (Long blockNumber : transitions.keySet()) {
      if (!isFirst) {
        stringBuilder.append(",");
      }
      isFirst = false;

      final Optional<Address> miningBeneficiary = transitions.get(blockNumber);
      stringBuilder.append("\n");
      stringBuilder.append(formatTransition(blockNumber, miningBeneficiary));
    }

    stringBuilder.append("\n]");
    stringBuilder.append("}\n");

    return stringBuilder.toString();
  }

  private String formatTransition(
      final long blockNumber, final Optional<Address> miningBeneficiary) {
    final StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("{");
    stringBuilder.append(
        formatKeyValues(
            "block",
            blockNumber,
            "miningbeneficiary",
            miningBeneficiary.map(addr -> addr.getBytes().toHexString()).orElse("")));
    stringBuilder.append("}");
    return stringBuilder.toString();
  }
}
