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
package org.hyperledger.besu.ethereum.core.encoding.receipt;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;

/**
 * The receipts of blocks of the bogota-devnet-1 network, where every transaction is an EIP-8141
 * frame transaction, as test vectors for the frame receipt encodings.
 *
 * <p>They were built from the {@code eth_getBlockReceipts} output of the devnet's geth node, one
 * receipt per line, in two forms:
 *
 * <ul>
 *   <li>{@link #eth69Receipts()}: the eth/69 network form, {@code [0x06, cumulative_gas_used,
 *       payer, frame_receipts]}, which geth and ethrex send in {@code Receipts} messages
 *   <li>{@link #typedReceipts()}: the consensus encoding, {@code 0x06 || rlp([cumulative_gas_used,
 *       payer, frame_receipts])}, which hashes to the receipts root of the block header. It is also
 *       the content of the RLP string that reth sends for a frame receipt
 * </ul>
 *
 * @param blockNumber the block number, in hex
 * @param receiptsRoot the receipts root of the block header
 * @param eth69Receipts the receipts in the eth/69 network form
 * @param typedReceipts the receipts in their consensus encoding
 */
public record BogotaDevnet1FrameReceipts(
    String blockNumber, Hash receiptsRoot, List<Bytes> eth69Receipts, List<Bytes> typedReceipts) {

  /** Block 0x190: 102 receipts, frames with every status (failure, success, skipped) and logs. */
  public static final BogotaDevnet1FrameReceipts BLOCK_0X190 =
      load("0x190", "0x4cc81ea481446e9fa52822476f254e36aa837fb455d1d2ba7872e6c2f2737a86");

  /** Block 0x843a: 11 receipts with logs. */
  public static final BogotaDevnet1FrameReceipts BLOCK_0X843A =
      load("0x843a", "0xb4c9b0c478e3903990821a0ac7bb146d274881f48398bf48865236940b7b376b");

  /**
   * All the blocks.
   *
   * @return the blocks
   */
  public static List<BogotaDevnet1FrameReceipts> all() {
    return List.of(BLOCK_0X190, BLOCK_0X843A);
  }

  /**
   * The receipts, decoded from their consensus encoding.
   *
   * @return the receipts
   */
  public List<TransactionReceipt> receipts() {
    return typedReceipts.stream()
        .map(
            receipt -> TransactionReceiptDecoder.readFrom(RLP.input(RLP.encodeOne(receipt)), false))
        .toList();
  }

  private static BogotaDevnet1FrameReceipts load(
      final String blockNumber, final String receiptsRoot) {
    return new BogotaDevnet1FrameReceipts(
        blockNumber,
        Hash.fromHexString(receiptsRoot),
        readReceipts("block-" + blockNumber + "-eth69-receipts.txt"),
        readReceipts("block-" + blockNumber + "-typed-receipts.txt"));
  }

  private static List<Bytes> readReceipts(final String fileName) {
    try (InputStream in =
            BogotaDevnet1FrameReceipts.class.getResourceAsStream("bogota-devnet-1/" + fileName);
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      return reader.lines().filter(line -> !line.isBlank()).map(Bytes::fromHexString).toList();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public String toString() {
    return "block " + blockNumber;
  }
}
