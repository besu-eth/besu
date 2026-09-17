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

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.LogWithMetadata;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.plugin.data.BlockBody;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;

public class BlockSimulationResult {
  final Block block;
  final BlockStateCallSimulationResult blockStateCallSimulationResult;
  final Optional<TrieLog> trieLog;
  final Optional<Function<TrieLog, Bytes>> trieLogSerializer;

  public BlockSimulationResult(
      final Block block, final BlockStateCallSimulationResult blockStateCallSimulationResult) {
    this.block = block;
    this.blockStateCallSimulationResult = blockStateCallSimulationResult;
    this.trieLog = Optional.empty();
    this.trieLogSerializer = Optional.empty();
  }

  public BlockSimulationResult(
      final Block block,
      final BlockStateCallSimulationResult blockStateCallSimulationResult,
      final TrieLog trieLog,
      final Function<TrieLog, Bytes> trieLogSerializer) {
    this.block = block;
    this.blockStateCallSimulationResult = blockStateCallSimulationResult;
    this.trieLog = Optional.ofNullable(trieLog);
    this.trieLogSerializer = Optional.ofNullable(trieLogSerializer);
  }

  public BlockHeader getBlockHeader() {
    return block.getHeader();
  }

  public BlockBody getBlockBody() {
    return block.getBody();
  }

  public List<TransactionReceipt> getReceipts() {
    return blockStateCallSimulationResult.getReceipts();
  }

  public List<TransactionSimulatorResult> getTransactionSimulations() {
    return blockStateCallSimulationResult.getTransactionSimulationResults();
  }

  public Block getBlock() {
    return block;
  }

  public List<LogWithMetadata> getLogsWithMetadata() {
    final List<LogWithMetadata> allLogs = new java.util.ArrayList<>();
    int logIndexOffset = 0;
    for (var transactionSimulation :
        blockStateCallSimulationResult.getTransactionSimulatorResults()) {
      final List<LogWithMetadata> txLogs =
          LogWithMetadata.generate(
              logIndexOffset,
              transactionSimulation.logs(),
              block.getHeader().getNumber(),
              block.getHash(),
              block.getHeader().getTimestamp(),
              transactionSimulation.result().transaction().getHash(),
              block
                  .getBody()
                  .getTransactions()
                  .indexOf(transactionSimulation.result().transaction()),
              false);
      allLogs.addAll(txLogs);
      // Advance offset by at least 1 per transaction so that failed transactions (0 logs)
      // still increment the log index, matching geth's eth_simulateV1 behaviour.
      logIndexOffset += Math.max(1, transactionSimulation.logs().size());
    }
    return allLogs;
  }

  public Optional<TrieLog> getTrieLog() {
    return trieLog;
  }

  public Optional<Bytes> getSerializedTrieLog() {
    return trieLogSerializer.flatMap(trieLog::map);
  }

  public Optional<BlockAccessList> getBlockAccessList() {
    return blockStateCallSimulationResult.getBlockAccessList();
  }
}
