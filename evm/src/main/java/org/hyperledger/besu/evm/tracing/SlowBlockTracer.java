/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.evm.tracing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.evm.worldstate.WorldView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class SlowBlockTracer implements OperationTracer {

  private static final Logger SLOW_BLOCK_LOG = LoggerFactory.getLogger("SlowBlock");
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private long executionStartNanos;
  private long executionTimeNanos;
  private int transactionCount;

  public void traceStartBlock() {
    executionStartNanos = System.nanoTime();
  }

  public void traceEndBlock(final long blockNumber, final Hash blockHash, final long gasUsed) {
    executionTimeNanos = System.nanoTime() - executionStartNanos;
    logSlowBlock(blockNumber, blockHash, gasUsed);
  }

  @Override
  public void traceEndTransaction(
      final WorldView worldView,
      final Transaction tx,
      final boolean status,
      final Bytes output,
      final List<Log> logs,
      final long gasUsed,
      final Set<Address> selfDestructs,
      final long timeNs) {
    transactionCount++;
  }

  private void logSlowBlock(final long blockNumber, final Hash blockHash, final long gasUsed) {
    try {
      final ObjectNode json = JSON_MAPPER.createObjectNode();
      json.put("level", "warn");
      json.put("msg", "Slow block");

      final ObjectNode blockNode = json.putObject("block");
      blockNode.put("number", blockNumber);
      blockNode.put("hash", blockHash.toHexString());
      blockNode.put("gas_used", gasUsed);
      blockNode.put("tx_count", transactionCount);

      final ObjectNode timingNode = json.putObject("timing");
      timingNode.put("execution_ms", executionTimeNanos / 1_000_000);
      timingNode.put("total_ms", executionTimeNanos / 1_000_000);

      SLOW_BLOCK_LOG.warn(JSON_MAPPER.writeValueAsString(json));
    } catch (JsonProcessingException e) {
      // Fallback to simple log

      SLOW_BLOCK_LOG.atWarn()
              .setMessage("Slow block number={} hash={} exec={}ms gas={} mgas/s={} txs={}")
              .addArgument(blockNumber)
              .addArgument(blockHash.toHexString())
              .addArgument(executionTimeNanos / 1_000_000)
              .addArgument(gasUsed)
              .addArgument(String.format("%.2f", calculateMGasPerSecond(gasUsed)))
              .addArgument(transactionCount)
              .log();
    }
  }

  private double calculateMGasPerSecond(final long gasUsed) {
    if (executionTimeNanos == 0) {
      return 0.00;
    }
    return (gasUsed / 1_000_000.0) / (executionTimeNanos / 1_000_000_000.0);
  }
}