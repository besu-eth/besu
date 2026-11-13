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
package org.hyperledger.besu.consensus.qbft.adaptor;

import org.hyperledger.besu.consensus.common.bft.SizeLimitedMap;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Outputs of blocks created or validated by this node, to not execute them again for validation or
 * import.
 *
 * <p>Block creation and proposal validation both save the block trie log. On import, whether by the
 * QBFT round or from the network, the importer only needs the receipts, and moves the head world
 * state with the trie log.
 *
 * <p>Also tracks the blocks being executed, so that a block is not executed by two threads at once:
 * importing a block waits for the validation of its proposal, and validating a proposal waits for
 * the import of the same block from the network.
 *
 * <p>Keyed by the on-chain hash, which ignores round and commit seals, so a proposal of any round
 * and its committed block share the entry.
 */
public class ValidatedBlockCache {

  private static final Logger LOG = LoggerFactory.getLogger(ValidatedBlockCache.class);

  /** Blocks of current height, plus few from round changes. */
  private static final int MAX_ENTRIES = 4;

  /** Waiting longer for another execution of a block than executing it is not useful. */
  private static final Duration MAX_EXECUTION_WAIT = Duration.ofSeconds(10);

  /**
   * Outputs needed by the import.
   *
   * @param blockNumber the block number
   * @param body the executed block body
   * @param receipts the receipts produced by processing the block
   * @param blockAccessList the block access list produced by processing the block, if any
   */
  public record ValidatedBlock(
      long blockNumber,
      BlockBody body,
      List<TransactionReceipt> receipts,
      Optional<BlockAccessList> blockAccessList) {

    /**
     * Check the body is the one executed.
     *
     * @param blockBody the body of the block to validate or import
     * @return true if it is the executed body
     */
    public boolean isExecutedBody(final BlockBody blockBody) {
      return isSameBody(body, blockBody);
    }
  }

  private final Map<Hash, ValidatedBlock> entries = new SizeLimitedMap<>(MAX_ENTRIES);

  /** Blocks being executed, by on-chain hash, completed when the execution ends. */
  private final Map<Hash, CompletableFuture<Void>> executions = new HashMap<>();

  /** Creates an empty cache. */
  public ValidatedBlockCache() {}

  /**
   * Check two bodies have the same content. A block received again from a peer, re-proposed or
   * announced with {@code NewBlock}, is decoded again. Transactions are compared by hash, which
   * covers their whole encoding, unlike {@code Transaction.equals}.
   *
   * @param body a block body
   * @param otherBody another block body
   * @return true if both bodies have the same transactions, ommers and withdrawals
   */
  public static boolean isSameBody(final BlockBody body, final BlockBody otherBody) {
    return transactionHashes(body).equals(transactionHashes(otherBody))
        && body.getOmmers().equals(otherBody.getOmmers())
        && body.getWithdrawals().equals(otherBody.getWithdrawals());
  }

  private static List<Hash> transactionHashes(final BlockBody blockBody) {
    return blockBody.getTransactions().stream().map(Transaction::getHash).toList();
  }

  /**
   * Add outputs of a created or validated block.
   *
   * @param blockHash the block hash
   * @param validatedBlock the block outputs
   */
  public synchronized void put(final Hash blockHash, final ValidatedBlock validatedBlock) {
    entries.put(blockHash, validatedBlock);
  }

  /**
   * Get outputs of a block, without removing them: the QBFT round and the network can both try to
   * import the block.
   *
   * @param blockHash the block hash
   * @return outputs if the block was created or validated
   */
  public synchronized Optional<ValidatedBlock> get(final Hash blockHash) {
    return Optional.ofNullable(entries.get(blockHash));
  }

  /**
   * Drop the outputs of blocks at or below the height of an imported block, as no other block can
   * be imported at those heights afterwards.
   *
   * @param blockNumber the number of the imported block
   */
  public synchronized void pruneUpTo(final long blockNumber) {
    entries.values().removeIf(entry -> entry.blockNumber() <= blockNumber);
  }

  /**
   * Record that the calling thread starts executing a block, unless another thread already does.
   *
   * @param blockHash the on-chain hash
   * @return true if the caller must execute the block and then call {@link #endExecution}, false if
   *     another thread is executing it, see {@link #awaitExecution}
   */
  public synchronized boolean tryStartExecution(final Hash blockHash) {
    if (executions.containsKey(blockHash)) {
      return false;
    }
    executions.put(blockHash, new CompletableFuture<>());
    return true;
  }

  /**
   * Record the end of an execution started with {@link #tryStartExecution}, successful or not, and
   * wake up the threads waiting for it.
   *
   * @param blockHash the on-chain hash
   */
  public void endExecution(final Hash blockHash) {
    final CompletableFuture<Void> execution;
    synchronized (this) {
      execution = executions.remove(blockHash);
    }
    if (execution != null) {
      execution.complete(null);
    }
  }

  /**
   * Wait for another thread executing a block, if any, so that its outputs can be used instead of
   * executing the block again. Returns after 10 seconds at most, or when interrupted, after which
   * the caller executes the block itself.
   *
   * @param blockHash the on-chain hash
   */
  public void awaitExecution(final Hash blockHash) {
    final CompletableFuture<Void> execution;
    synchronized (this) {
      execution = executions.get(blockHash);
    }
    if (execution == null) {
      return;
    }
    try {
      execution.get(MAX_EXECUTION_WAIT.toMillis(), TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (final ExecutionException | TimeoutException e) {
      LOG.debug("Stopped waiting for another execution of block {}", blockHash, e);
    }
  }
}
