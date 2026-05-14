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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptDecoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class TransactionReceiptTest {

  @Test
  public void toFromRlp() {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final TransactionReceipt receipt = gen.receipt();
    final TransactionReceipt copy =
        TransactionReceiptDecoder.readFrom(
            RLP.input(
                RLP.encode(
                    output ->
                        TransactionReceiptEncoder.writeTo(
                            receipt,
                            output,
                            TransactionReceiptEncodingConfiguration
                                .DEFAULT_NETWORK_CONFIGURATION))),
            false);
    assertThat(copy).isEqualTo(receipt);
  }

  @Test
  public void toFromRlpWithReason() {
    final TransactionReceiptEncodingConfiguration encodingOptions =
        new TransactionReceiptEncodingConfiguration.Builder().withRevertReason(true).build();

    final BlockDataGenerator gen = new BlockDataGenerator();
    final TransactionReceipt receipt = gen.receipt(Bytes.fromHexString("0x1122334455667788"));
    final TransactionReceipt copy =
        TransactionReceiptDecoder.readFrom(
            RLP.input(
                RLP.encode(
                    rlpOut -> TransactionReceiptEncoder.writeTo(receipt, rlpOut, encodingOptions))),
            true);
    assertThat(copy).isEqualTo(receipt);
  }

  @Test
  public void toFromRlpCompacted() {
    final TransactionReceiptEncodingConfiguration encodingOptions =
        new TransactionReceiptEncodingConfiguration.Builder()
            .withCompactedLogs(true)
            .withBloomFilter(false)
            .build();

    final BlockDataGenerator gen = new BlockDataGenerator();
    final TransactionReceipt receipt = gen.receipt(Bytes.fromHexString("0x1122334455667788"));
    final TransactionReceipt copy =
        TransactionReceiptDecoder.readFrom(
            RLP.input(
                RLP.encode(
                    rlpOut -> TransactionReceiptEncoder.writeTo(receipt, rlpOut, encodingOptions))),
            true);
    assertThat(copy).isEqualTo(receipt);
  }

  @Test
  public void toFromRlpCompactedWithReason() {
    final TransactionReceiptEncodingConfiguration encodingOptions =
        new TransactionReceiptEncodingConfiguration.Builder()
            .withRevertReason(true)
            .withCompactedLogs(true)
            .withBloomFilter(false)
            .build();

    final BlockDataGenerator gen = new BlockDataGenerator();
    final TransactionReceipt receipt = gen.receipt(Bytes.fromHexString("0x1122334455667788"));
    final TransactionReceipt copy =
        TransactionReceiptDecoder.readFrom(
            RLP.input(
                RLP.encode(
                    rlpOut -> TransactionReceiptEncoder.writeTo(receipt, rlpOut, encodingOptions))),
            true);
    assertThat(copy).isEqualTo(receipt);
  }

  @Test
  public void uncompactedAndCompactedDecodeToSameReceipt() {

    final TransactionReceiptEncodingConfiguration encodingOptionsWithCompaction =
        new TransactionReceiptEncodingConfiguration.Builder()
            .withCompactedLogs(true)
            .withBloomFilter(false)
            .build();

    final TransactionReceiptEncodingConfiguration encodingOptionsWithoutCompaction =
        new TransactionReceiptEncodingConfiguration.Builder()
            .withCompactedLogs(false)
            .withBloomFilter(true)
            .build();

    final BlockDataGenerator gen = new BlockDataGenerator();
    final TransactionReceipt receipt = gen.receipt(Bytes.fromHexString("0x1122334455667788"));
    final Bytes compactedReceipt =
        RLP.encode(
            rlpOut ->
                TransactionReceiptEncoder.writeTo(receipt, rlpOut, encodingOptionsWithCompaction));
    final Bytes unCompactedReceipt =
        RLP.encode(
            rlpOut ->
                TransactionReceiptEncoder.writeTo(
                    receipt, rlpOut, encodingOptionsWithoutCompaction));
    assertThat(TransactionReceiptDecoder.readFrom(RLP.input(compactedReceipt), true))
        .isEqualTo(receipt);
    assertThat(TransactionReceiptDecoder.readFrom(RLP.input(unCompactedReceipt), true))
        .isEqualTo(receipt);
  }

  @Test
  public void toFromRlpEth69Receipt() {
    final TransactionReceiptEncodingConfiguration encodingConfiguration =
        TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION;

    final BlockDataGenerator gen = new BlockDataGenerator();
    final TransactionReceipt receipt = gen.receipt(Bytes.fromHexString("0x1122334455667788"));
    final TransactionReceipt copy =
        TransactionReceiptDecoder.readFrom(
            RLP.input(
                RLP.encode(
                    rlpOut ->
                        TransactionReceiptEncoder.writeTo(receipt, rlpOut, encodingConfiguration))),
            true);
    assertThat(copy).isEqualTo(receipt);
  }

  @Test
  public void decodeEth69WithEmptyStringForType() {
    String encodedReceiptWith0x00AsType =
        "0xf85800808844e52a8ce6476327f84be494fccc2c35f0b84609e5f12c55dd85aba8d5d9bef7c08d72e5900112b81927ba5bb5f67ee594b4049bf0e4aed78db15d7bf2fc0c34e9a99de4efc08e8137ad659878f9e93df1f658367a";
    String encodedReceiptWith0x80AsType =
        "0xf85880808844e52a8ce6476327f84be494fccc2c35f0b84609e5f12c55dd85aba8d5d9bef7c08d72e5900112b81927ba5bb5f67ee594b4049bf0e4aed78db15d7bf2fc0c34e9a99de4efc08e8137ad659878f9e93df1f658367a";
    final TransactionReceipt with0x00 =
        TransactionReceiptDecoder.readFrom(
            RLP.input(Bytes.fromHexString(encodedReceiptWith0x00AsType)), false);
    final TransactionReceipt with0x80 =
        TransactionReceiptDecoder.readFrom(
            RLP.input(Bytes.fromHexString(encodedReceiptWith0x80AsType)), false);
    assertThat(with0x00).isEqualTo(with0x80);
  }

  @Test
  public void encodeEth69FrontierTypeAs0x80() {
    final TransactionReceiptEncodingConfiguration encodingConfiguration =
        TransactionReceiptEncodingConfiguration.ETH69_RECEIPT_CONFIGURATION;
    final BlockDataGenerator gen = new BlockDataGenerator();
    final TransactionReceipt frontierReceipt =
        new TransactionReceipt(
            TransactionType.FRONTIER, 1, 1L, List.of(gen.log()), Optional.empty());

    final Bytes encodedReceipt =
        RLP.encode(
            rlpOut ->
                TransactionReceiptEncoder.writeTo(frontierReceipt, rlpOut, encodingConfiguration));

    final RLPInput input = RLP.input(encodedReceipt);
    input.enterList();
    assertThat(input.readBytes()).isEqualTo(Bytes.EMPTY);
    assertThat(RLP.encode(out -> out.writeBytes(Bytes.EMPTY))).isEqualTo(Bytes.of(0x80));
  }
}
