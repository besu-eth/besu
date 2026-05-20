/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.TransactionType.EIP1559;
import static org.hyperledger.besu.datatypes.TransactionType.FRONTIER;
import static org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION;
import static org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION;
import static org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration.STORAGE_WITHOUT_COMPACTION;
import static org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration.STORAGE_WITH_COMPACTION;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptDecoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TransactionReceiptEncodingConfigurationTest {

  private static final Bytes REVERT_REASON = Bytes.fromHexString("0x1122334455667788");

  private static Stream<Object[]> provider() {
    return Stream.of(
        new Object[] {"DEFAULT_NETWORK_CONFIGURATION", DEFAULT_NETWORK_CONFIGURATION},
        new Object[] {"ETH69_RECEIPT_CONFIGURATION", ETH69_RECEIPT_CONFIGURATION},
        new Object[] {"STORAGE_WITH_COMPACTION", STORAGE_WITH_COMPACTION},
        new Object[] {"STORAGE_WITHOUT_COMPACTION", STORAGE_WITHOUT_COMPACTION});
  }

  @ParameterizedTest(name = "{0}={1}")
  @MethodSource("provider")
  public void toFromRlpLegacyTransactionSuccessfulStatus(
      final String ignored, final TransactionReceiptEncodingConfiguration encodingOptions) {
    final TransactionReceipt receipt = createTransactionReceiptStatus(FRONTIER, 1);
    Bytes encoded =
        RLP.encode(rlpOut -> TransactionReceiptEncoder.writeTo(receipt, rlpOut, encodingOptions));
    final TransactionReceipt copy = TransactionReceiptDecoder.readFrom(RLP.input(encoded), true);
    assertThat(copy).isEqualTo(receipt);
  }

  @ParameterizedTest(name = "{0}={1}")
  @MethodSource("provider")
  public void toFromRlpLegacyTransactionFailedStatus(
      final String ignored, final TransactionReceiptEncodingConfiguration encodingOptions) {
    final TransactionReceipt receipt = createTransactionReceiptStatus(FRONTIER, 0);
    Bytes encoded =
        RLP.encode(rlpOut -> TransactionReceiptEncoder.writeTo(receipt, rlpOut, encodingOptions));
    final TransactionReceipt copy = TransactionReceiptDecoder.readFrom(RLP.input(encoded), true);
    assertThat(copy).isEqualTo(receipt);
  }

  @ParameterizedTest(name = "{0}={1}")
  @MethodSource("provider")
  public void toFromRlpLegacyTransactionStateRoot(
      final String ignored, final TransactionReceiptEncodingConfiguration encodingOptions) {
    final TransactionReceipt receipt = createTransactionReceiptStateRoot(FRONTIER);
    Bytes encoded =
        RLP.encode(rlpOut -> TransactionReceiptEncoder.writeTo(receipt, rlpOut, encodingOptions));
    final TransactionReceipt copy = TransactionReceiptDecoder.readFrom(RLP.input(encoded), true);
    assertThat(copy).isEqualTo(receipt);
  }

  @ParameterizedTest(name = "{0}={1}")
  @MethodSource("provider")
  public void toFrom1559TransactionSuccessfulStatus(
      final String ignored, final TransactionReceiptEncodingConfiguration encodingOptions) {

    final TransactionReceipt receipt = createTransactionReceiptStatus(EIP1559, 1);
    Bytes encoded =
        RLP.encode(rlpOut -> TransactionReceiptEncoder.writeTo(receipt, rlpOut, encodingOptions));
    final TransactionReceipt copy = TransactionReceiptDecoder.readFrom(RLP.input(encoded), true);
    assertThat(copy).isEqualTo(receipt);
  }

  @ParameterizedTest(name = "{0}={1}")
  @MethodSource("provider")
  public void toFrom1559TransactionFailedStatus(
      final String ignored, final TransactionReceiptEncodingConfiguration encodingOptions) {
    final TransactionReceipt receipt = createTransactionReceiptStatus(EIP1559, 0);
    Bytes encoded =
        RLP.encode(rlpOut -> TransactionReceiptEncoder.writeTo(receipt, rlpOut, encodingOptions));
    final TransactionReceipt copy = TransactionReceiptDecoder.readFrom(RLP.input(encoded), true);
    assertThat(copy).isEqualTo(receipt);
  }

  @ParameterizedTest(name = "{0}={1}")
  @MethodSource("provider")
  public void toFrom1559TransactionStateRoot(
      final String ignored, final TransactionReceiptEncodingConfiguration encodingOptions) {
    final TransactionReceipt receipt = createTransactionReceiptStateRoot(EIP1559);
    Bytes encoded =
        RLP.encode(rlpOut -> TransactionReceiptEncoder.writeTo(receipt, rlpOut, encodingOptions));
    final TransactionReceipt copy = TransactionReceiptDecoder.readFrom(RLP.input(encoded), true);
    assertThat(copy).isEqualTo(receipt);
  }

  @ParameterizedTest(name = "{0}={1}")
  @MethodSource("provider")
  public void frontierReceiptFirstFieldEncodingDependsOnReceiptFormat(
      final String ignored, final TransactionReceiptEncodingConfiguration encodingOptions) {
    final TransactionReceipt receipt = createTransactionReceiptStatus(FRONTIER, 1);
    final Bytes encoded =
        RLP.encode(rlpOut -> TransactionReceiptEncoder.writeTo(receipt, rlpOut, encodingOptions));

    final RLPInput input = RLP.input(encoded);
    input.enterList();
    final Bytes firstField = input.readBytes();

    if (encodingOptions.isWithEth69Receipt()) {
      assertThat(firstField)
          .as(
              "eth69 includes the tx type, and frontier type must be encoded as empty string (0x80)")
          .isEqualTo(Bytes.EMPTY);
    } else {
      assertThat(firstField)
          .as("legacy receipt encoding has no tx type prefix; first field is receipt status")
          .isEqualTo(Bytes.of((byte) 1));
    }
  }

  private TransactionReceipt createTransactionReceiptStatus(
      final TransactionType transactionType, final int status) {
    final BlockDataGenerator gen = new BlockDataGenerator();
    long cumulativeGasUsed = 1L;
    return new TransactionReceipt(
        transactionType,
        status,
        cumulativeGasUsed,
        Collections.singletonList(gen.log()),
        Optional.of(REVERT_REASON));
  }

  private TransactionReceipt createTransactionReceiptStateRoot(
      final TransactionType transactionType) {
    final BlockDataGenerator gen = new BlockDataGenerator();
    long cumulativeGasUsed = 1L;
    return new TransactionReceipt(
        transactionType,
        Hash.EMPTY_TRIE_HASH,
        cumulativeGasUsed,
        Collections.singletonList(gen.log()),
        LogsBloomFilter.builder().insertLogs(Collections.singletonList(gen.log())).build(),
        Optional.of(REVERT_REASON));
  }
}
