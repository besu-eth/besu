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
package org.hyperledger.besu.ethereum.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.BlockStateCallSimulationResult.TransactionSimulatorResultWithMetadata;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class BlockSimulationResultTest {

  @Test
  public void callsWithoutLogsUseNoLogIndex() {
    final List<List<Log>> logsPerCall =
        List.of(List.of(), List.of(), List.of(log()), List.of(), List.of(log(), log()));

    final List<Transaction> transactions = new ArrayList<>();
    final List<TransactionSimulatorResultWithMetadata> results = new ArrayList<>();
    for (final List<Log> logs : logsPerCall) {
      final Transaction transaction = mock(Transaction.class);
      when(transaction.getHash()).thenReturn(Hash.ZERO);
      transactions.add(transaction);
      results.add(
          new TransactionSimulatorResultWithMetadata(
              new TransactionSimulatorResult(transaction, mock(TransactionProcessingResult.class)),
              mock(TransactionReceipt.class),
              0L,
              logs));
    }

    final BlockStateCallSimulationResult blockStateCallResult =
        mock(BlockStateCallSimulationResult.class);
    when(blockStateCallResult.getTransactionSimulatorResults()).thenReturn(results);
    final BlockBody body = mock(BlockBody.class);
    when(body.getTransactions()).thenReturn(transactions);
    final Block block = new Block(mock(BlockHeader.class), body);

    final List<LogWithMetadata> logs =
        new BlockSimulationResult(block, blockStateCallResult).getLogsWithMetadata();

    assertThat(logs).extracting(LogWithMetadata::getLogIndex).containsExactly(0, 1, 2);
    assertThat(logs).extracting(LogWithMetadata::getTransactionIndex).containsExactly(2, 4, 4);
  }

  @Test
  public void equalHashCallsKeepTheirOwnTransactionIndexes() {
    final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    final BigInteger halfCurveOrder = signatureAlgorithm.getHalfCurveOrder();
    final SECPSignature signature =
        signatureAlgorithm.createSignature(halfCurveOrder, halfCurveOrder, (byte) 0);
    final Address toAddress = Address.fromHexString("0xc100000000000000000000000000000000000000");

    final Transaction txA =
        Transaction.builder()
            .nonce(0)
            .gasLimit(100_000L)
            .to(toAddress)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .gasPrice(Wei.ZERO)
            .signature(signature)
            .sender(Address.fromHexString("0xc000000000000000000000000000000000000000"))
            .build();
    final Transaction txB =
        Transaction.builder()
            .nonce(0)
            .gasLimit(100_000L)
            .to(toAddress)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .gasPrice(Wei.ZERO)
            .signature(signature)
            .sender(Address.fromHexString("0xc200000000000000000000000000000000000000"))
            .build();

    assertThat(txA).isNotSameAs(txB);
    assertThat(txA).isEqualTo(txB);
    assertThat(txA.getHash()).isEqualTo(txB.getHash());

    final List<Transaction> transactions = List.of(txA, txB);
    final List<TransactionSimulatorResultWithMetadata> results =
        List.of(
            new TransactionSimulatorResultWithMetadata(
                new TransactionSimulatorResult(txA, mock(TransactionProcessingResult.class)),
                mock(TransactionReceipt.class),
                0L,
                List.of(log())),
            new TransactionSimulatorResultWithMetadata(
                new TransactionSimulatorResult(txB, mock(TransactionProcessingResult.class)),
                mock(TransactionReceipt.class),
                0L,
                List.of(log())));

    final BlockStateCallSimulationResult blockStateCallResult =
        mock(BlockStateCallSimulationResult.class);
    when(blockStateCallResult.getTransactionSimulatorResults()).thenReturn(results);
    final BlockBody body = mock(BlockBody.class);
    when(body.getTransactions()).thenReturn(transactions);
    final Block block = new Block(mock(BlockHeader.class), body);

    final List<LogWithMetadata> logs =
        new BlockSimulationResult(block, blockStateCallResult).getLogsWithMetadata();

    assertThat(logs).extracting(LogWithMetadata::getTransactionIndex).containsExactly(0, 1);
  }

  @Test
  public void equalHashCallsWithEmptyMiddleSlotKeepPositionsAndIndexes() {
    final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    final BigInteger halfCurveOrder = signatureAlgorithm.getHalfCurveOrder();
    final SECPSignature signature =
        signatureAlgorithm.createSignature(halfCurveOrder, halfCurveOrder, (byte) 0);
    final Address toAddress = Address.fromHexString("0xc100000000000000000000000000000000000000");

    final Transaction txA =
        Transaction.builder()
            .nonce(0)
            .gasLimit(100_000L)
            .to(toAddress)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .gasPrice(Wei.ZERO)
            .signature(signature)
            .sender(Address.fromHexString("0xc000000000000000000000000000000000000000"))
            .build();
    final Transaction txB =
        Transaction.builder()
            .nonce(0)
            .gasLimit(100_000L)
            .to(toAddress)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .gasPrice(Wei.ZERO)
            .signature(signature)
            .sender(Address.fromHexString("0xc100000000000000000000000000000000000001"))
            .build();
    final Transaction txC =
        Transaction.builder()
            .nonce(0)
            .gasLimit(100_000L)
            .to(toAddress)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .gasPrice(Wei.ZERO)
            .signature(signature)
            .sender(Address.fromHexString("0xc200000000000000000000000000000000000000"))
            .build();

    // All three are equal (same hash) but distinct objects.
    assertThat(txA).isNotSameAs(txB);
    assertThat(txA).isEqualTo(txB);
    assertThat(txA.getHash()).isEqualTo(txB.getHash());
    assertThat(txA).isNotSameAs(txC);
    assertThat(txA).isEqualTo(txC);
    assertThat(txA.getHash()).isEqualTo(txC.getHash());

    final List<Transaction> transactions = List.of(txA, txB, txC);
    final List<TransactionSimulatorResultWithMetadata> results =
        List.of(
            // call 0: one log
            new TransactionSimulatorResultWithMetadata(
                new TransactionSimulatorResult(txA, mock(TransactionProcessingResult.class)),
                mock(TransactionReceipt.class),
                0L,
                List.of(log())),
            // call 1: empty slot
            new TransactionSimulatorResultWithMetadata(
                new TransactionSimulatorResult(txB, mock(TransactionProcessingResult.class)),
                mock(TransactionReceipt.class),
                0L,
                List.of()),
            // call 2: one log
            new TransactionSimulatorResultWithMetadata(
                new TransactionSimulatorResult(txC, mock(TransactionProcessingResult.class)),
                mock(TransactionReceipt.class),
                0L,
                List.of(log())));

    final BlockStateCallSimulationResult blockStateCallResult =
        mock(BlockStateCallSimulationResult.class);
    when(blockStateCallResult.getTransactionSimulatorResults()).thenReturn(results);
    final BlockBody body = mock(BlockBody.class);
    when(body.getTransactions()).thenReturn(transactions);
    final Block block = new Block(mock(BlockHeader.class), body);

    final List<LogWithMetadata> logs =
        new BlockSimulationResult(block, blockStateCallResult).getLogsWithMetadata();

    // Empty middle slot contributes no entries; exactly 2 flat log entries.
    assertThat(logs).hasSize(2);
    // logIndex accumulates across emitted logs only (0 for logA, 1 for logB).
    assertThat(logs).extracting(LogWithMetadata::getLogIndex).containsExactly(0, 1);
    // transactionIndex is positional: logA→call0=0, logB→call2=2.
    // On old (indexOf) code both resolve to 0 → RED on old code.
    assertThat(logs).extracting(LogWithMetadata::getTransactionIndex).containsExactly(0, 2);
  }

  @Test
  public void equalHashCallsWithUnequalLogCountsAccumulateOffsets() {
    final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    final BigInteger halfCurveOrder = signatureAlgorithm.getHalfCurveOrder();
    final SECPSignature signature =
        signatureAlgorithm.createSignature(halfCurveOrder, halfCurveOrder, (byte) 0);
    final Address toAddress = Address.fromHexString("0xc100000000000000000000000000000000000000");

    final Transaction txA =
        Transaction.builder()
            .nonce(0)
            .gasLimit(100_000L)
            .to(toAddress)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .gasPrice(Wei.ZERO)
            .signature(signature)
            .sender(Address.fromHexString("0xc000000000000000000000000000000000000000"))
            .build();
    final Transaction txB =
        Transaction.builder()
            .nonce(0)
            .gasLimit(100_000L)
            .to(toAddress)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .gasPrice(Wei.ZERO)
            .signature(signature)
            .sender(Address.fromHexString("0xc200000000000000000000000000000000000000"))
            .build();

    // Equal hashes, distinct objects.
    assertThat(txA).isNotSameAs(txB);
    assertThat(txA).isEqualTo(txB);
    assertThat(txA.getHash()).isEqualTo(txB.getHash());

    final List<Transaction> transactions = List.of(txA, txB);
    final List<TransactionSimulatorResultWithMetadata> results =
        List.of(
            // call 0: two logs
            new TransactionSimulatorResultWithMetadata(
                new TransactionSimulatorResult(txA, mock(TransactionProcessingResult.class)),
                mock(TransactionReceipt.class),
                0L,
                List.of(log(), log())),
            // call 1: one log
            new TransactionSimulatorResultWithMetadata(
                new TransactionSimulatorResult(txB, mock(TransactionProcessingResult.class)),
                mock(TransactionReceipt.class),
                0L,
                List.of(log())));

    final BlockStateCallSimulationResult blockStateCallResult =
        mock(BlockStateCallSimulationResult.class);
    when(blockStateCallResult.getTransactionSimulatorResults()).thenReturn(results);
    final BlockBody body = mock(BlockBody.class);
    when(body.getTransactions()).thenReturn(transactions);
    final Block block = new Block(mock(BlockHeader.class), body);

    final List<LogWithMetadata> logs =
        new BlockSimulationResult(block, blockStateCallResult).getLogsWithMetadata();

    assertThat(logs).hasSize(3);
    // logIndex accumulates across the whole block: 0, 1 (call0), 2 (call1).
    assertThat(logs).extracting(LogWithMetadata::getLogIndex).containsExactly(0, 1, 2);
    // transactionIndex: call0's logs → 0, call1's log → 1.
    // On old (indexOf) code call1 resolves to 0 → RED on old code.
    assertThat(logs).extracting(LogWithMetadata::getTransactionIndex).containsExactly(0, 0, 1);
  }

  private static Log log() {
    return new Log(Address.ZERO, Bytes.EMPTY, List.of());
  }
}
