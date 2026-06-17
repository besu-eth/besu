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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.AbstractCallOperation;
import org.hyperledger.besu.evm.operation.AbstractCreateOperation;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.SLoadOperation;
import org.hyperledger.besu.evm.operation.SStoreOperation;
import org.hyperledger.besu.evm.worldstate.WorldView;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tracer that collects execution metrics and logs slow blocks.
 *
 * <p>The tracer implements the cross-client execution metrics specification, collecting detailed
 * statistics about block execution including timing, state access patterns, cache performance, and
 * EVM operation counts. Blocks exceeding the configured threshold are logged in a standardized JSON
 * format.
 *
 * <p>The tracer uses a dedicated "SlowBlock" logger, allowing operators to route slow block output
 * to a separate file/sink via logback configuration.
 */
public class SlowBlockTracer implements OperationTracer, StateAccessTracer {

  private final long thresholdMs;

  /**
   * Creates a tracer that logs blocks whose total processing time meets the given threshold.
   *
   * @param thresholdMs blocks are logged when total processing time in ms &ge; this value (0 logs
   *     every block)
   */
  public SlowBlockTracer(final long thresholdMs) {
    this.thresholdMs = thresholdMs;
  }

  private static final Logger SLOW_BLOCK_LOG = LoggerFactory.getLogger("SlowBlock");
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private int transactionCount;
  // Timing
  private long stateReadTimeNanos;
  private long stateHashTimeNanos;
  private long commitTimeNanos;
  private long totalStartNanos;
  private long totalTimeNanos;
  // EVM operation counters
  private int sloadCount;
  private int sstoreCount;
  private int callCount;
  private int createCount;
  // Unique tracking
  private final Set<Address> uniqueAccountsTouched = new HashSet<>();
  private final Set<StorageSlotKey> uniqueStorageSlots = new HashSet<>();
  private final Set<Address> uniqueContractsExecuted = new HashSet<>();
  // State read cache counters (accumulator in-block cache for accounts/storage; CodeCache for code)
  private int accountCacheHits;
  private int accountCacheMisses;
  private int storageCacheHits;
  private int storageCacheMisses;
  private int codeCacheHits;
  private int codeCacheMisses;
  private int codeBytesRead;
  // State write counters (net unique changes per block, set via addStateWriteCounts)
  private int accountWrites;
  private int storageWrites;
  private int codeWrites;
  private int codeBytesWritten;
  // State delete counts
  private int accountDeletes;
  private int storageDeletes;

  /** Supports slow block timing metrics, distinct from BlockAwareOperationTracer */
  public void traceStartBlock() {
    totalStartNanos = System.nanoTime();
  }

  /**
   * Completes the slow block metrics and triggers a log event.
   *
   * <p>Whereas BlockAwareOperationTracer.traceEndBlock() is called before worldState.persist(),
   * this callsite is after worldState.persist() so we capture persist and state hash calculation
   * metrics.
   *
   * @param blockNumber the block number
   * @param blockHash the block hash
   * @param gasUsed the gas used at the end of the block (after refunds)
   */
  public void traceEndBlockPersist(
      final long blockNumber, final Hash blockHash, final long gasUsed) {
    totalTimeNanos = System.nanoTime() - totalStartNanos;
    if (totalTimeNanos / 1_000_000 >= thresholdMs) {
      logSlowBlock(blockNumber, blockHash, gasUsed);
    }
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

  @Override
  public void tracePreExecution(final MessageFrame frame) {
    final var op = frame.getCurrentOperation();
    if (op instanceof SLoadOperation || op instanceof SStoreOperation) { // TODO SLD EVMv2 support
      final Address storageAddress = frame.getRecipientAddress();
      // TODO SLD EVMv2 needs to read from v2 stack
      // TODO SLD convert to UInt256 or can leave as Bytes? (Note: Bytes32 errors for some reason)
      final Bytes slotKey = frame.getStackItem(0);
      uniqueStorageSlots.add(new StorageSlotKey(storageAddress, slotKey));
      uniqueAccountsTouched.add(storageAddress);
    }
  }

  private record StorageSlotKey(Address address, Bytes slotKey) {}

  @Override
  public void tracePostExecution(
      final MessageFrame frame, final Operation.OperationResult operationResult) {
    switch (frame.getCurrentOperation()) {
      // TODO SLD EVMv2 support
      case SLoadOperation _ -> sloadCount++;
      case SStoreOperation _ -> sstoreCount++;
      case AbstractCallOperation _ -> callCount++; // CALL, CALLCODE, DELEGATECALL, STATICCALL
      case AbstractCreateOperation _ -> createCount++; // CREATE, CREATE2
      default -> {} // No tracking needed for other operations
    }
  }

  @Override
  public void traceContextEnter(final MessageFrame frame) {
    final Address recipient = frame.getRecipientAddress();
    if (recipient != null) {
      uniqueContractsExecuted.add(recipient);
      uniqueAccountsTouched.add(recipient);
    }

    final Address sender = frame.getSenderAddress();
    if (sender != null) {
      uniqueAccountsTouched.add(recipient);
    }
  }

  /**
   * records time in nanos for state_hash_ms
   *
   * @param timeNanos the time in nanos
   */
  public void addStateHashTime(final long timeNanos) {
    stateHashTimeNanos = timeNanos;
  }

  /**
   * records time in nanos for commit_ms
   *
   * @param timeNanos the time in nanos
   */
  public void addCommitTime(final long timeNanos) {
    commitTimeNanos = timeNanos;
  }

  /**
   * Records the net per-block state write counts, called from {@code PathBasedWorldState.persist()}
   * before the accumulator is reset.
   *
   * @param accounts number of account entries that changed (including deletions)
   * @param storageSlots number of storage slot entries that changed (including deletions)
   * @param code number of code entries that changed (including deletions)
   * @param codeBytes number of bytes of code written
   * @param accountDeletes number of account entries deleted (subset of {@code accounts})
   * @param storageDeletes number of storage slot entries deleted (subset of {@code storageSlots})
   */
  public void addStateWriteCounts(
      final int accounts,
      final int storageSlots,
      final int code,
      final int codeBytes,
      final int accountDeletes,
      final int storageDeletes) {
    accountWrites = accounts;
    storageWrites = storageSlots;
    codeWrites = code;
    codeBytesWritten = codeBytes;
    this.accountDeletes = accountDeletes;
    this.storageDeletes = storageDeletes;
  }

  @Override
  public void traceAccountRead(final boolean cacheHit) {
    if (cacheHit) {
      accountCacheHits++;
    } else {
      accountCacheMisses++;
    }
  }

  @Override
  public void addStateReadTime(final long timeNs) {
    stateReadTimeNanos += timeNs;
  }

  @Override
  public void traceStorageRead(final boolean cacheHit) {
    if (cacheHit) {
      storageCacheHits++;
    } else {
      storageCacheMisses++;
    }
  }

  @Override
  public void traceCodeRead(final boolean cacheHit) {
    if (cacheHit) {
      codeCacheHits++;
    } else {
      codeCacheMisses++;
    }
  }

  @Override
  public void addCodeBytesRead(final int size) {
    codeBytesRead += size;
  }

  private void logSlowBlock(final long blockNumber, final Hash blockHash, final long gasUsed) {
    String formattedMGasPerSecond = formatTwoDecimalPlaces(calculateMGasPerSecond(gasUsed));
    long executionTimeNanos = totalTimeNanos - stateHashTimeNanos - commitTimeNanos;
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
      timingNode.put("execution_ms", formatTwoDecimalPlaces(executionTimeNanos / 1_000_000.0));
      timingNode.put("state_read_ms", formatTwoDecimalPlaces(stateReadTimeNanos / 1_000_000.0));
      timingNode.put("state_hash_ms", formatTwoDecimalPlaces(stateHashTimeNanos / 1_000_000.0));
      timingNode.put("commit_ms", formatTwoDecimalPlaces(commitTimeNanos / 1_000_000.0));
      timingNode.put("total_ms", formatTwoDecimalPlaces(totalTimeNanos / 1_000_000.0));

      final ObjectNode throughputNode = json.putObject("throughput");
      throughputNode.put("mgas_per_sec", formattedMGasPerSecond);

      final ObjectNode uniqueNode = json.putObject("unique");
      uniqueNode.put("accounts", uniqueAccountsTouched.size());
      uniqueNode.put("storage_slots", uniqueStorageSlots.size());
      uniqueNode.put("contracts", uniqueContractsExecuted.size());

      final ObjectNode evmNode = json.putObject("evm");
      evmNode.put("sload", sloadCount);
      evmNode.put("sstore", sstoreCount);
      evmNode.put("calls", callCount);
      evmNode.put("creates", createCount);

      final ObjectNode stateReadsNode = json.putObject("state_reads");
      stateReadsNode.put("accounts", accountCacheHits + accountCacheMisses);
      stateReadsNode.put("storage_slots", storageCacheHits + storageCacheMisses);
      stateReadsNode.put("code", codeCacheHits + codeCacheMisses);
      stateReadsNode.put("code_bytes", codeBytesRead);

      final ObjectNode stateWritesNode = json.putObject("state_writes");
      stateWritesNode.put("accounts", accountWrites);
      stateWritesNode.put("storage_slots", storageWrites);
      stateWritesNode.put("code", codeWrites);
      stateWritesNode.put("code_bytes", codeBytesWritten);
      stateWritesNode.put("accounts_deleted", accountDeletes);
      stateWritesNode.put("storage_slots_deleted", storageDeletes);

      final ObjectNode cacheNode = json.putObject("cache");
      final ObjectNode accountCacheNode = cacheNode.putObject("account");
      accountCacheNode.put("hits", accountCacheHits);
      accountCacheNode.put("misses", accountCacheMisses);
      accountCacheNode.put("hit_rate", calculateHitRate(accountCacheHits, accountCacheMisses));
      final ObjectNode storageCacheNode = cacheNode.putObject("storage");
      storageCacheNode.put("hits", storageCacheHits);
      storageCacheNode.put("misses", storageCacheMisses);
      storageCacheNode.put("hit_rate", calculateHitRate(storageCacheHits, storageCacheMisses));
      final ObjectNode codeCacheNode = cacheNode.putObject("code");
      codeCacheNode.put("hits", codeCacheHits);
      codeCacheNode.put("misses", codeCacheMisses);
      codeCacheNode.put("hit_rate", calculateHitRate(codeCacheHits, codeCacheMisses));

      SLOW_BLOCK_LOG.warn(JSON_MAPPER.writeValueAsString(json));
    } catch (JsonProcessingException e) {
      // Fallback to simple log
      SLOW_BLOCK_LOG
          .atWarn()
          .setMessage("Slow block number={} hash={} exec={}ms gas={} mgas/s={} txs={}")
          .addArgument(blockNumber)
          .addArgument(blockHash.toHexString())
          .addArgument(formatTwoDecimalPlaces(totalTimeNanos / 1_000_000.0))
          .addArgument(gasUsed)
          .addArgument(formattedMGasPerSecond)
          .addArgument(transactionCount)
          .log();
    }
  }

  private double calculateMGasPerSecond(final long gasUsed) {
    if (totalTimeNanos == 0) {
      return 0.00;
    }
    return (gasUsed / 1_000_000.0) / (totalTimeNanos / 1_000_000_000.0);
  }

  private String formatTwoDecimalPlaces(final double value) {
    return String.format("%.2f", value);
  }

  /**
   * Calculates the cache hit rate as a percentage.
   *
   * @param hits the number of cache hits
   * @param misses the number of cache misses
   * @return the hit rate as a percentage (0-100)
   */
  private static double calculateHitRate(final long hits, final long misses) {
    final long total = hits + misses;
    if (total > 0) {
      return Math.round((hits * 100.0) / total * 100.0) / 100.0;
    }
    return 0.0;
  }
}
