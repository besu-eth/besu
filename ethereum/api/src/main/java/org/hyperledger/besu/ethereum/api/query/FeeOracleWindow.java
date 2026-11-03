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
package org.hyperledger.besu.ethereum.api.query;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.util.OrderStatistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * The gas prices and priority fees of the transactions in the last {@code --api-gas-price-blocks}
 * blocks up to a chain head, from which {@code eth_gasPrice} and {@code eth_maxPriorityFeePerGas}
 * take a percentile.
 *
 * <p>The fees of the most recent window's blocks are kept, keyed by block hash, so a new head only
 * reads the blocks that are not in it: the new block, or the blocks of a new branch.
 */
final class FeeOracleWindow {

  // How many of the window's blocks keep their fees between heads; it does not change which
  // blocks the percentiles are taken over. A window of up to this many blocks is kept whole, so a
  // new head reads one block. A larger window keeps its newest blocks, and its older blocks are
  // read for every head, so memory stays bounded for any --api-gas-price-blocks.
  static final int MAX_RETAINED_BLOCKS = 1024;

  private static final long[] NO_VALUES = new long[0];
  private static final Wei[] NO_LARGE_VALUES = new Wei[0];
  private static final FeeValues NO_FEE_VALUES = new FeeValues(NO_VALUES, NO_LARGE_VALUES);

  /** The percentiles of the window's gas prices and priority fees, empty if it has none. */
  record Percentiles(Optional<Wei> gasPrice, Optional<Wei> priorityFee) {}

  // Fees below 2^63 are kept as longs, which take a tenth of the memory of Wei objects.
  private record FeeValues(long[] values, Wei[] largeValues) {}

  private record BlockFees(Hash parentHash, FeeValues gasPrices, FeeValues priorityFees) {}

  private record Window(long headNumber, Map<Hash, BlockFees> blocks) {}

  private final Blockchain blockchain;
  private final long windowBlocks;
  private final double percentile;
  private final AtomicReference<Window> retained = new AtomicReference<>(new Window(-1, Map.of()));

  FeeOracleWindow(final Blockchain blockchain, final long windowBlocks, final double percentile) {
    this.blockchain = blockchain;
    this.windowBlocks = windowBlocks;
    this.percentile = percentile;
  }

  /**
   * Takes the percentiles over the window of blocks that ends at the given head.
   *
   * @param head the last block of the window
   * @return the percentiles, or empty if a block of the window cannot be read
   */
  Optional<Percentiles> percentilesAt(final BlockHeader head) {
    final long firstBlock = Math.max(0, head.getNumber() - windowBlocks + 1);
    final int blockCount = (int) (head.getNumber() - firstBlock + 1);
    final Map<Hash, BlockFees> previous = retained.get().blocks();
    final Map<Hash, BlockFees> retain =
        HashMap.newHashMap(Math.min(blockCount, MAX_RETAINED_BLOCKS));
    final List<BlockFees> blocks = new ArrayList<>(blockCount);

    Hash hash = head.getBlockHash();
    for (long number = head.getNumber(); number >= firstBlock; number--) {
      BlockFees fees = previous.get(hash);
      if (fees == null) {
        final Optional<BlockHeader> header =
            number == head.getNumber() ? Optional.of(head) : blockchain.getBlockHeader(hash);
        final Optional<BlockBody> body =
            header.isPresent() ? blockchain.getBlockBody(hash) : Optional.empty();
        if (body.isEmpty()) {
          // A partial window would bias the percentile; callers fall back to the configured lower
          // bound.
          return Optional.empty();
        }
        fees = blockFeesOf(header.get(), body.get());
      }
      if (blocks.size() < MAX_RETAINED_BLOCKS) {
        retain.put(hash, fees);
      }
      blocks.add(fees);
      hash = fees.parentHash();
    }

    // A computation for an older head can finish after one for a newer head; keeping the older
    // window would make the next head read blocks again. The chain head's window is always kept,
    // so after a reorg to a lower head the next heads of that branch read only their own block.
    final boolean isChainHead = head.getBlockHash().equals(blockchain.getChainHeadHash());
    retained.accumulateAndGet(
        new Window(head.getNumber(), retain),
        (current, next) ->
            isChainHead || next.headNumber() >= current.headNumber() ? next : current);

    return Optional.of(
        new Percentiles(
            percentileOf(blocks, BlockFees::gasPrices),
            percentileOf(blocks, BlockFees::priorityFees)));
  }

  private Optional<Wei> percentileOf(
      final List<BlockFees> blocks, final Function<BlockFees, FeeValues> valuesOf) {
    int valueCount = 0;
    int largeValueCount = 0;
    for (final BlockFees block : blocks) {
      final FeeValues feeValues = valuesOf.apply(block);
      valueCount += feeValues.values().length;
      largeValueCount += feeValues.largeValues().length;
    }
    final int count = valueCount + largeValueCount;
    if (count == 0) {
      return Optional.empty();
    }
    final int k = Math.min(count - 1, (int) (count * percentile));

    // The selection reorders its input, so it runs on a copy: the blocks' arrays are kept for later
    // heads.
    if (k < valueCount) {
      final long[] values = new long[valueCount];
      int offset = 0;
      for (final BlockFees block : blocks) {
        final long[] blockValues = valuesOf.apply(block).values();
        System.arraycopy(blockValues, 0, values, offset, blockValues.length);
        offset += blockValues.length;
      }
      return Optional.of(Wei.of(OrderStatistics.selectKthInPlace(values, k)));
    }
    // Every large value is above every value that fits a long.
    final Wei[] largeValues = new Wei[largeValueCount];
    int offset = 0;
    for (final BlockFees block : blocks) {
      final Wei[] blockValues = valuesOf.apply(block).largeValues();
      System.arraycopy(blockValues, 0, largeValues, offset, blockValues.length);
      offset += blockValues.length;
    }
    return Optional.of(OrderStatistics.selectKthInPlace(largeValues, k - valueCount));
  }

  private static BlockFees blockFeesOf(final BlockHeader header, final BlockBody body) {
    final List<Transaction> transactions = body.getTransactions();
    return new BlockFees(
        header.getParentHash(),
        feeValuesOf(transactions, Transaction::getGasPrice),
        feeValuesOf(transactions, Transaction::getMaxPriorityFeePerGas));
  }

  private static FeeValues feeValuesOf(
      final List<Transaction> transactions, final Function<Transaction, Optional<Wei>> feeOf) {
    int valueCount = 0;
    int largeValueCount = 0;
    for (final Transaction transaction : transactions) {
      final Optional<Wei> fee = feeOf.apply(transaction);
      if (fee.isPresent()) {
        if (fee.get().fitsLong()) {
          valueCount++;
        } else {
          largeValueCount++;
        }
      }
    }
    if (valueCount == 0 && largeValueCount == 0) {
      return NO_FEE_VALUES;
    }

    final long[] values = valueCount == 0 ? NO_VALUES : new long[valueCount];
    final Wei[] largeValues = largeValueCount == 0 ? NO_LARGE_VALUES : new Wei[largeValueCount];
    int valueIndex = 0;
    int largeValueIndex = 0;
    for (final Transaction transaction : transactions) {
      final Optional<Wei> fee = feeOf.apply(transaction);
      if (fee.isPresent()) {
        if (fee.get().fitsLong()) {
          values[valueIndex++] = fee.get().toLong();
        } else {
          largeValues[largeValueIndex++] = fee.get();
        }
      }
    }
    return new FeeValues(values, largeValues);
  }
}
