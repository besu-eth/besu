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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.BlockStateCallSimulationResult.TransactionSimulatorResultWithMetadata;

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

  private static Log log() {
    return new Log(Address.ZERO, Bytes.EMPTY, List.of());
  }
}
