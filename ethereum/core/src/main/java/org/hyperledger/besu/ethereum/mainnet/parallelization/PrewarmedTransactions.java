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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tuweni.bytes.Bytes;
import org.jspecify.annotations.Nullable;

/**
 * The transactions the mempool prewarmer executed on the state of the head, kept until the head
 * changes so that the next block can take a result instead of executing the transaction again. See
 * {@link PrewarmReuse} for when a result stands for an execution.
 */
public class PrewarmedTransactions {

  /** Keeps nothing, so no block takes a result. */
  public static final PrewarmedTransactions DISABLED =
      new PrewarmedTransactions(false, new NoOpMetricsSystem());

  /**
   * The fee recipient of prewarm executions. The block's coinbase is not known before the block, so
   * the fee goes to an address no transaction is expected to touch, and moves to the coinbase when
   * a block takes the result.
   */
  public static final Address PREWARM_COINBASE =
      Address.fromHexString("0x7e570000000000000000000000000000c0ffee00");

  /** Bounds the executions kept for one head. */
  private static final int MAX_EXECUTIONS = 20_000;

  private static final int COINBASE = 0x41;
  private static final int TIMESTAMP = 0x42;
  private static final int PREVRANDAO = 0x44;
  private static final int GASLIMIT = 0x45;
  private static final int PUSH1 = 0x60;
  private static final int PUSH32 = 0x7f;

  /** An operation the code of an execution contains that reads the block's coinbase. */
  public static final int READS_COINBASE = 1;

  /** An operation the code of an execution contains that reads the block's timestamp. */
  public static final int READS_TIMESTAMP = 1 << 1;

  /** An operation the code of an execution contains that reads the block's prevRandao. */
  public static final int READS_PREVRANDAO = 1 << 2;

  /** An operation the code of an execution contains that reads the block's gas limit. */
  public static final int READS_GAS_LIMIT = 1 << 3;

  /**
   * A transaction the prewarmer executed.
   *
   * @param head the head whose state it ran on
   * @param timestamp the timestamp of the block it assumed
   * @param gasLimit the gas limit of the block it assumed
   * @param blockValueReads which block values its code can read, as {@code READS_*} bits
   * @param previous the execution of the sender's transaction before it, whose writes it read
   * @param result its result, with the accounts and slots it read and wrote
   */
  public record Execution(
      Hash head,
      long timestamp,
      long gasLimit,
      int blockValueReads,
      @Nullable Execution previous,
      TransactionProcessingResult result) {}

  private final boolean enabled;
  private final Map<Hash, Execution> executions = new ConcurrentHashMap<>();
  private volatile Hash head = Hash.ZERO;
  private volatile Hash lastBlockReusing = Hash.ZERO;
  private volatile Hash blockWithoutReuse = Hash.ZERO;
  private final Counter reusedTransactions;
  private final Counter blocksProcessedAgain;

  /**
   * Keeps prewarm executions for the blocks to reuse.
   *
   * @param metricsSystem the metrics system
   */
  public PrewarmedTransactions(final MetricsSystem metricsSystem) {
    this(true, metricsSystem);
  }

  private PrewarmedTransactions(final boolean enabled, final MetricsSystem metricsSystem) {
    this.enabled = enabled;
    this.reusedTransactions =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCK_PROCESSING,
            "mempool_prewarm_reused_transactions_total",
            "Block transactions whose prewarm result was taken instead of executing them");
    this.blocksProcessedAgain =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCK_PROCESSING,
            "mempool_prewarm_reuse_blocks_processed_again_total",
            "Blocks that failed after taking prewarm results and were processed again without");
  }

  /**
   * Whether prewarm executions are kept.
   *
   * @return whether prewarm executions are kept
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Keeps a prewarm execution until the head changes.
   *
   * @param transaction the transaction
   * @param execution its execution
   */
  public void record(final Hash transaction, final Execution execution) {
    if (!enabled) {
      return;
    }
    if (!head.equals(execution.head())) {
      synchronized (executions) {
        if (!head.equals(execution.head())) {
          executions.clear();
          head = execution.head();
        }
      }
    }
    if (executions.size() < MAX_EXECUTIONS) {
      executions.put(transaction, execution);
    }
  }

  /**
   * The reuse of prewarm results for a block, if there are results for its parent.
   *
   * @param block the block
   * @return the reuse for the block
   */
  public Optional<PrewarmReuse> forBlock(final BlockHeader block) {
    if (!enabled
        || executions.isEmpty()
        || !head.equals(block.getParentHash())
        || blockWithoutReuse.equals(block.getHash())) {
      return Optional.empty();
    }
    return Optional.of(new PrewarmReuse(this, block));
  }

  /**
   * Whether the block took a prewarm result the last time it was processed.
   *
   * @param block the block
   * @return whether it did
   */
  public boolean reusedIn(final Hash block) {
    return enabled && lastBlockReusing.equals(block);
  }

  /**
   * Has the block processed without taking prewarm results, after it failed with them.
   *
   * @param block the block
   */
  public void processWithoutReuse(final Hash block) {
    blockWithoutReuse = block;
    lastBlockReusing = Hash.ZERO;
    blocksProcessedAgain.inc();
  }

  Optional<Execution> execution(final Hash transaction) {
    return Optional.ofNullable(executions.get(transaction));
  }

  void reused(final BlockHeader block) {
    lastBlockReusing = block.getHash();
    reusedTransactions.inc();
  }

  /**
   * Which block values the code can read: an operation that reads one anywhere in the code, outside
   * push data, counts even if the execution never reached it.
   *
   * @param codes the code the execution ran
   * @return the {@code READS_*} bits
   */
  public static int blockValueReads(final Collection<Code> codes) {
    int reads = 0;
    for (final Code code : codes) {
      reads |= blockValueReads(code.getBytes());
    }
    return reads;
  }

  static int blockValueReads(final Bytes code) {
    int reads = 0;
    final int size = code.size();
    for (int i = 0; i < size; i++) {
      final int opcode = code.get(i) & 0xff;
      switch (opcode) {
        case COINBASE -> reads |= READS_COINBASE;
        case TIMESTAMP -> reads |= READS_TIMESTAMP;
        case PREVRANDAO -> reads |= READS_PREVRANDAO;
        case GASLIMIT -> reads |= READS_GAS_LIMIT;
        default -> {
          if (opcode >= PUSH1 && opcode <= PUSH32) {
            i += opcode - PUSH1 + 1;
          }
        }
      }
    }
    return reads;
  }
}
