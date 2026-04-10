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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.datatypes.AccessListEntry;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.IstanbulGasCalculator;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;
import org.hyperledger.besu.evm.gascalculator.ShanghaiGasCalculator;

import java.util.List;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class IntrinsicGasTest {

  public static Stream<Arguments> data() {
    final GasCalculator frontier = new FrontierGasCalculator();
    final GasCalculator istanbul = new IstanbulGasCalculator();
    final GasCalculator shanghai = new ShanghaiGasCalculator();
    final GasCalculator prague = new PragueGasCalculator();
    return Stream.of(
        // EnoughGAS
        Arguments.of(
            frontier,
            21952L,
            "0xf86d80018259d894095e7baea6a6c7c4c2dfeb977efac326af552d870a8e0358ac39584bc98a7c979f984b031ba048b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a36649353a01fffd310ac743f371de3b9f7f9cb56c0b28ad43601b4ab949f53faa07bd2c804"),
        Arguments.of(
            istanbul,
            21224L,
            "0xf86d80018259d894095e7baea6a6c7c4c2dfeb977efac326af552d870a8e0358ac39584bc98a7c979f984b031ba048b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a36649353a01fffd310ac743f371de3b9f7f9cb56c0b28ad43601b4ab949f53faa07bd2c804"),
        // FirstZeroBytes
        Arguments.of(
            frontier,
            21180L,
            "0xf87c80018261a894095e7baea6a6c7c4c2dfeb977efac326af552d870a9d00000000000000000000000000010000000000000000000000000000001ba048b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a36649353a01fffd310ac743f371de3b9f7f9cb56c0b28ad43601b4ab949f53faa07bd2c804"),
        Arguments.of(
            istanbul,
            21128L,
            "0xf87c80018261a894095e7baea6a6c7c4c2dfeb977efac326af552d870a9d00000000000000000000000000010000000000000000000000000000001ba048b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a36649353a01fffd310ac743f371de3b9f7f9cb56c0b28ad43601b4ab949f53faa07bd2c804"),
        // LastZeroBytes
        Arguments.of(
            frontier,
            21180L,
            "0xf87c80018261a894095e7baea6a6c7c4c2dfeb977efac326af552d870a9d01000000000000000000000000000000000000000000000000000000001ba048b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a36649353a01fffd310ac743f371de3b9f7f9cb56c0b28ad43601b4ab949f53faa07bd2c804"),
        Arguments.of(
            istanbul,
            21128L,
            "0xf87c80018261a894095e7baea6a6c7c4c2dfeb977efac326af552d870a9d01000000000000000000000000000000000000000000000000000000001ba048b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a36649353a01fffd310ac743f371de3b9f7f9cb56c0b28ad43601b4ab949f53faa07bd2c804"),
        // NotEnoughGAS
        Arguments.of(
            frontier,
            21952L,
            "0xf86d800182521c94095e7baea6a6c7c4c2dfeb977efac326af552d870a8e0358ac39584bc98a7c979f984b031ba048b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a36649353a0efffd310ac743f371de3b9f7f9cb56c0b28ad43601b4ab949f53faa07bd2c804"),
        Arguments.of(
            istanbul,
            21224L,
            "0xf86d800182521c94095e7baea6a6c7c4c2dfeb977efac326af552d870a8e0358ac39584bc98a7c979f984b031ba048b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a36649353a0efffd310ac743f371de3b9f7f9cb56c0b28ad43601b4ab949f53faa07bd2c804"),
        // ZeroBytes
        Arguments.of(
            frontier,
            21116L,
            "0xf87c80018261a894095e7baea6a6c7c4c2dfeb977efac326af552d870a9d00000000000000000000000000000000000000000000000000000000001ba048b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a36649353a01fffd310ac743f371de3b9f7f9cb56c0b28ad43601b4ab949f53faa07bd2c804"),
        Arguments.of(
            istanbul,
            21116L,
            "0xf87c80018261a894095e7baea6a6c7c4c2dfeb977efac326af552d870a9d00000000000000000000000000000000000000000000000000000000001ba048b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a36649353a01fffd310ac743f371de3b9f7f9cb56c0b28ad43601b4ab949f53faa07bd2c804"),
        // CallData Gas Increase
        Arguments.of(
            prague,
            21116L,
            "0xf87c80018261a894095e7baea6a6c7c4c2dfeb977efac326af552d870a9d00000000000000000000000000000000000000000000000000000000001ba048b55bfa915ac795c431978d8a6a992b628d557da5ff759b307d495a36649353a01fffd310ac743f371de3b9f7f9cb56c0b28ad43601b4ab949f53faa07bd2c804"),
        // AccessList
        Arguments.of(
            shanghai,
            25300L,
            "0x01f89a018001826a4094095e7baea6a6c7c4c2dfeb977efac326af552d878080f838f794a95e7baea6a6c7c4c2dfeb977efac326af552d87e1a0ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff80a05cbd172231fc0735e0fb994dd5b1a4939170a260b36f0427a8a80866b063b948a07c230f7f578dd61785c93361b9871c0706ebfa6d06e3f4491dc9558c5202ed36"));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void validateGasCost(
      final GasCalculator gasCalculator, final long expectedGas, final String txRlp) {
    Bytes rlp = Bytes.fromHexString(txRlp);

    // non-frontier transactions need to be opaque for parsing to work
    if (rlp.get(0) > 0) {
      final BytesValueRLPOutput output = new BytesValueRLPOutput();
      output.writeBytes(rlp);
      rlp = output.encoded();
    }

    Transaction t = Transaction.readFrom(RLP.input(rlp));
    Assertions.assertThat(
            gasCalculator.transactionIntrinsicGasCost(t, baselineGas(gasCalculator, t)))
        .isEqualTo(expectedGas);
  }

  long baselineGas(final GasCalculator gasCalculator, final Transaction transaction) {
    final List<AccessListEntry> accessListEntries = transaction.getAccessList().orElse(List.of());

    int accessListStorageCount = 0;
    for (final var entry : accessListEntries) {
      final List<Bytes32> storageKeys = entry.storageKeys();
      accessListStorageCount += storageKeys.size();
    }
    final long accessListGas =
        gasCalculator.accessListGasCost(accessListEntries.size(), accessListStorageCount);

    final long codeDelegationGas =
        gasCalculator.delegateCodeGasCost(transaction.codeDelegationListSize());

    return accessListGas + codeDelegationGas;
  }
}
