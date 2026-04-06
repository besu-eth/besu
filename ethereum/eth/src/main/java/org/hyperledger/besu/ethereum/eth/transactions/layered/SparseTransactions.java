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
package org.hyperledger.besu.ethereum.eth.transactions.layered;

import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredRemovalReason.LayerMoveReason.PROMOTED;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredRemovalReason.PoolRemovalReason.INVALIDATED;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredRemovalReason.PoolRemovalReason;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import com.google.common.collect.Iterables;

public class SparseTransactions extends AbstractTransactionsLayer {
  private static final String SENDER_MIXED_PRIORITY_ERROR =
      "Sender %s cannot simultaneously have and not have priority."
          + " Probably you need to set --tx-pool-no-local-priority=true and/or configure"
          + " the list of priority senders with --tx-pool-priority-senders";

  /**
   * Order sparse tx by priority flag and sequence asc, so that we pick for eviction txs that have
   * no priority and with the lowest sequence number (oldest) first.
   */
  private final NavigableSet<PendingTransaction> sparseEvictionOrder =
      new TreeSet<>(
          Comparator.comparing(PendingTransaction::hasPriority)
              .thenComparing(PendingTransaction::getSequence));

  private final Map<Address, Integer> gapBySender = new HashMap<>();
  private final List<SendersByPriority> orderByGap;

  public SparseTransactions(
      final TransactionPoolConfiguration poolConfig,
      final EthScheduler ethScheduler,
      final TransactionsLayer nextLayer,
      final TransactionPoolMetrics metrics,
      final BiFunction<PendingTransaction, PendingTransaction, Boolean>
          transactionReplacementTester,
      final BlobCache blobCache) {
    super(poolConfig, ethScheduler, nextLayer, transactionReplacementTester, metrics, blobCache);
    orderByGap = new ArrayList<>(poolConfig.getMaxFutureBySender());
    IntStream.range(0, poolConfig.getMaxFutureBySender())
        .forEach(i -> orderByGap.add(new SendersByPriority()));
  }

  @Override
  public String name() {
    return "sparse";
  }

  @Override
  protected long cacheFreeSpace() {
    return poolConfig.getPendingTransactionsLayerMaxCapacityBytes() - getLayerSpaceUsed();
  }

  @Override
  protected boolean gapsAllowed() {
    return true;
  }

  @Override
  public void reset() {
    super.reset();
    sparseEvictionOrder.clear();
    gapBySender.clear();
    orderByGap.forEach(SendersByPriority::clear);
  }

  @Override
  protected TransactionAddedResult canAdd(
      final PendingTransaction pendingTransaction, final int gap) {
    gapBySender.compute(
        pendingTransaction.getSender(),
        (sender, currGap) -> {
          if (currGap == null) {
            orderByGap.get(gap).add(pendingTransaction);
            return gap;
          }
          if (Long.compareUnsigned(
                  pendingTransaction.getNonce(), txsBySender.get(sender).firstKey())
              < 0) {
            orderByGap.get(currGap).remove(sender);
            orderByGap.get(gap).add(pendingTransaction);
            return gap;
          }
          return currGap;
        });

    return TransactionAddedResult.ADDED;
  }

  @Override
  protected void internalAdd(
      final NavigableMap<Long, PendingTransaction> senderTxs, final PendingTransaction addedTx) {
    sparseEvictionOrder.add(addedTx);
  }

  @Override
  protected int maxTransactionsNumber() {
    return Integer.MAX_VALUE;
  }

  @Override
  protected void internalReplaced(final PendingTransaction replacedTx) {
    sparseEvictionOrder.remove(replacedTx);
  }

  @Override
  protected void internalBlockAdded(final BlockHeader blockHeader, final FeeMarket feeMarket) {}

  /**
   * We only want to promote transactions that have gap == 0, so there will be no gap in the prev
   * layers. A promoted transaction is removed from this layer, and the gap data is updated for its
   * sender.
   *
   * @param promotionFilter the prev layer's promotion filter
   * @param freeSpace max amount of memory promoted txs can occupy
   * @param freeSlots max number of promoted txs
   * @return a list of transactions promoted to the prev layer
   */
  @Override
  public List<PendingTransaction> promote(
      final Predicate<PendingTransaction> promotionFilter,
      final long freeSpace,
      final int freeSlots,
      final int[] remainingPromotionsPerType) {
    long accumulatedSpace = 0;
    final List<PendingTransaction> promotedTxs = new ArrayList<>();

    final var zeroGapSenders = orderByGap.get(0);

    search:
    for (final var sender : zeroGapSenders) {
      final var senderSeqTxs = getSequentialSubset(txsBySender.get(sender));

      for (final var candidateTx : senderSeqTxs.values()) {
        final var txType = candidateTx.getTransaction().getType();
        if (promotionFilter.test(candidateTx) && remainingPromotionsPerType[txType.ordinal()] > 0) {
          accumulatedSpace += candidateTx.memorySize();
          if (promotedTxs.size() < freeSlots && accumulatedSpace <= freeSpace) {
            promotedTxs.add(candidateTx);
            --remainingPromotionsPerType[txType.ordinal()];
          } else {
            // no room for more txs the search is over exit the loops
            break search;
          }
        } else {
          // skip remaining txs for this sender
          break;
        }
      }
    }

    // remove promoted txs from this layer
    promotedTxs.forEach(
        promotedTx -> {
          final var sender = promotedTx.getSender();
          final var senderTxs = txsBySender.get(sender);
          senderTxs.remove(promotedTx.getNonce());
          processRemove(senderTxs, promotedTx.getTransaction(), PROMOTED);
          if (senderTxs.isEmpty()) {
            txsBySender.remove(sender);
            orderByGap.get(0).remove(sender);
            gapBySender.remove(sender);
          } else {
            final long firstNonce = senderTxs.firstKey();
            final int newGap = (int) (firstNonce - (promotedTx.getNonce() + 1));
            if (newGap != 0) {
              updateGap(sender, 0, newGap);
            }
          }
        });

    if (!promotedTxs.isEmpty()) {
      // since we removed some txs we can try to promote from next layer
      promoteTransactions();
    }

    return promotedTxs;
  }

  @Override
  public PendingTransactions.Status getStatus() {
    final PendingTransactions.Status nextLayerStatus = nextLayer.getStatus();
    long pendingCount = nextLayerStatus.pendingCount();
    long queueCount = nextLayerStatus.queuedCount();
    for (final Map.Entry<Address, Integer> entry : gapBySender.entrySet()) {
      final Address sender = entry.getKey();
      final int gap = entry.getValue();
      if (gap == 0) {
        pendingCount += txsBySender.get(sender).size();
      } else {
        queueCount += txsBySender.get(sender).size();
      }
    }
    return new PendingTransactions.Status(pendingCount, queueCount);
  }

  private NavigableMap<Long, PendingTransaction> getSequentialSubset(
      final NavigableMap<Long, PendingTransaction> senderTxs) {
    long lastSequentialNonce = senderTxs.firstKey();
    for (final long nonce : senderTxs.tailMap(lastSequentialNonce, false).keySet()) {
      if (nonce == lastSequentialNonce + 1) {
        ++lastSequentialNonce;
      } else {
        break;
      }
    }
    return senderTxs.headMap(lastSequentialNonce, true);
  }

  @Override
  public synchronized void remove(
      final PendingTransaction invalidatedTx, final PoolRemovalReason reason) {

    final var senderTxs = txsBySender.get(invalidatedTx.getSender());
    if (senderTxs != null && senderTxs.containsKey(invalidatedTx.getNonce())) {
      // gaps are allowed here then just remove
      senderTxs.remove(invalidatedTx.getNonce());
      processRemove(senderTxs, invalidatedTx.getTransaction(), reason);
      if (senderTxs.isEmpty()) {
        txsBySender.remove(invalidatedTx.getSender());
      }
    } else {
      nextLayer.remove(invalidatedTx, reason);
    }
  }

  @Override
  protected void internalConfirmed(
      final NavigableMap<Long, PendingTransaction> senderTxs,
      final Address sender,
      final long maxConfirmedNonce,
      final PendingTransaction highestNonceRemovedTx) {

    if (highestNonceRemovedTx != null) {
      final int currGap = gapBySender.get(sender);
      final int newGap = (int) (senderTxs.firstKey() - (highestNonceRemovedTx.getNonce() + 1));
      if (currGap != newGap) {
        updateGap(sender, currGap, newGap);
      }
    } else {
      final int currGap = gapBySender.get(sender);
      final int newGap = (int) (senderTxs.firstKey() - (maxConfirmedNonce + 1));
      if (newGap < currGap) {
        updateGap(sender, currGap, newGap);
      }
    }
  }

  @Override
  protected void internalEvict(
      final NavigableMap<Long, PendingTransaction> lessReadySenderTxs,
      final PendingTransaction evictedTx) {
    sparseEvictionOrder.remove(evictedTx);

    if (lessReadySenderTxs.isEmpty()) {
      deleteGap(evictedTx.getSender());
    }
  }

  @Override
  protected void internalRemove(
      final NavigableMap<Long, PendingTransaction> senderTxs,
      final PendingTransaction removedTx,
      final LayeredRemovalReason removalReason) {

    sparseEvictionOrder.remove(removedTx);

    final Address sender = removedTx.getSender();

    if (senderTxs != null && !senderTxs.isEmpty()) {
      final int deltaGap = (int) (senderTxs.firstKey() - removedTx.getNonce());
      if (deltaGap > 0) {
        final int currGap = gapBySender.get(sender);
        final int newGap;
        if (removalReason.equals(INVALIDATED)) {
          newGap = currGap + deltaGap;
        } else {
          newGap = deltaGap - 1;
        }
        if (currGap != newGap) {
          updateGap(sender, currGap, newGap);
        }
      }

    } else {
      deleteGap(sender);
    }
  }

  @Override
  protected void internalPenalize(final PendingTransaction penalizedTx) {
    // intentionally no-op
  }

  private void deleteGap(final Address sender) {
    orderByGap.get(gapBySender.remove(sender)).remove(sender);
  }

  @Override
  protected PendingTransaction getEvictable() {
    return sparseEvictionOrder.first();
  }

  @Override
  protected boolean promotionFilter(final PendingTransaction pendingTransaction) {
    return false;
  }

  /**
   * Return the full content of this layer, organized as a list of sender pending txs. For each
   * sender the collection pending txs is ordered by nonce asc.
   *
   * <p>Returned sender list order detail: first the sender of the tx that will be evicted as last.
   * So for example if the same sender has the first and the last txs in the eviction order, it will
   * be the first in the returned list, since we give precedence to tx that will be evicted later.
   *
   * @return a list of sender pending txs
   */
  @Override
  public List<List<PendingTransaction>> getBySender() {
    final var sendersToAdd = new HashSet<>(txsBySender.keySet());
    return sparseEvictionOrder.descendingSet().stream()
        .map(PendingTransaction::getSender)
        .filter(sendersToAdd::remove)
        .map(sender -> List.copyOf(txsBySender.get(sender).values()))
        .toList();
  }

  @Override
  public OptionalLong getNextNonceFor(final Address sender) {
    final Integer gap = gapBySender.get(sender);
    if (gap != null && gap == 0) {
      final var senderTxs = txsBySender.get(sender);
      var currNonce = senderTxs.firstKey();
      for (final var nextNonce : senderTxs.keySet()) {
        if (nextNonce > currNonce + 1) {
          break;
        }
        currNonce = nextNonce;
      }
      return OptionalLong.of(currNonce + 1);
    }
    return OptionalLong.empty();
  }

  @Override
  public OptionalLong getCurrentNonceFor(final Address sender) {
    final var senderTxs = txsBySender.get(sender);
    if (senderTxs != null) {
      final var gap = gapBySender.get(sender);
      return OptionalLong.of(senderTxs.firstKey() - gap);
    }
    return nextLayer.getCurrentNonceFor(sender);
  }

  @Override
  protected void internalNotifyAdded(
      final NavigableMap<Long, PendingTransaction> senderTxs,
      final PendingTransaction pendingTransaction) {
    final Address sender = pendingTransaction.getSender();
    final Integer currGap = gapBySender.get(sender);
    if (currGap != null) {
      final int newGap = (int) (senderTxs.firstKey() - (pendingTransaction.getNonce() + 1));
      if (newGap < currGap) {
        updateGap(sender, currGap, newGap);
      }
    }
  }

  @Override
  public String logSender(final Address sender) {
    final var senderTxs = txsBySender.get(sender);
    return name()
        + "["
        + (Objects.isNull(senderTxs)
            ? "Empty"
            : "gap(" + gapBySender.get(sender) + ") " + senderTxs.keySet())
        + "] "
        + nextLayer.logSender(sender);
  }

  @Override
  public String internalLogStats() {
    if (sparseEvictionOrder.isEmpty()) {
      return "Sparse: Empty";
    }

    final Transaction newest = sparseEvictionOrder.last().getTransaction();
    final Transaction oldest = sparseEvictionOrder.first().getTransaction();

    return "Sparse: "
        + "count="
        + pendingTransactions.size()
        + ", space used: "
        + spaceUsed
        + ", unique senders: "
        + txsBySender.size()
        + ", oldest [gap: "
        + gapBySender.get(oldest.getSender())
        + ", max fee:"
        + oldest.getMaxGasPrice().toHumanReadableString()
        + ", hash: "
        + oldest.getHash()
        + "], newest [gap: "
        + gapBySender.get(newest.getSender())
        + ", max fee: "
        + newest.getMaxGasPrice().toHumanReadableString()
        + ", hash: "
        + newest.getHash()
        + "]";
  }

  private void updateGap(final Address sender, final int currGap, final int newGap) {
    final boolean hasPriority = orderByGap.get(currGap).remove(sender);
    orderByGap.get(newGap).add(sender, hasPriority);
    gapBySender.put(sender, newGap);
  }

  @Override
  protected void internalConsistencyCheck(
      final Map<Address, NavigableMap<Long, PendingTransaction>> prevLayerTxsBySender) {
    txsBySender.values().stream()
        .filter(senderTxs -> senderTxs.size() > 1)
        .map(NavigableMap::entrySet)
        .map(Set::iterator)
        .forEach(
            itNonce -> {
              PendingTransaction firstTx = itNonce.next().getValue();

              prevLayerTxsBySender.computeIfPresent(
                  firstTx.getSender(),
                  (sender, txsByNonce) -> {
                    final long prevLayerMaxNonce = txsByNonce.lastKey();
                    assert prevLayerMaxNonce < firstTx.getNonce()
                        : "first nonce is not greater than previous layer last nonce";

                    final int gap = (int) (firstTx.getNonce() - (prevLayerMaxNonce + 1));
                    assert gapBySender.get(firstTx.getSender()).equals(gap) : "gap mismatch";
                    assert orderByGap.get(gap).contains(firstTx.getSender())
                        : "orderByGap sender not found";

                    return txsByNonce;
                  });

              long prevNonce = firstTx.getNonce();

              while (itNonce.hasNext()) {
                final long currNonce = itNonce.next().getKey();
                assert Long.compareUnsigned(prevNonce, currNonce) < 0 : "non incremental nonce";
                prevNonce = currNonce;
              }
            });
  }

  private static class SendersByPriority implements Iterable<Address> {
    final Set<Address> prioritySenders = new HashSet<>();
    final Set<Address> standardSenders = new HashSet<>();

    void clear() {
      prioritySenders.clear();
      standardSenders.clear();
    }

    public void add(final Address sender, final boolean hasPriority) {
      if (hasPriority) {
        if (standardSenders.contains(sender)) {
          throw new IllegalStateException(SENDER_MIXED_PRIORITY_ERROR.formatted(sender));
        }
        prioritySenders.add(sender);
      } else {
        if (prioritySenders.contains(sender)) {
          throw new IllegalStateException(SENDER_MIXED_PRIORITY_ERROR.formatted(sender));
        }
        standardSenders.add(sender);
      }
    }

    void add(final PendingTransaction pendingTransaction) {
      add(pendingTransaction.getSender(), pendingTransaction.hasPriority());
    }

    boolean remove(final Address sender) {
      if (standardSenders.remove(sender)) {
        return false;
      }
      return prioritySenders.remove(sender);
    }

    public boolean contains(final Address sender) {
      return standardSenders.contains(sender) || prioritySenders.contains(sender);
    }

    @Override
    public Iterator<Address> iterator() {
      return Iterables.concat(prioritySenders, standardSenders).iterator();
    }
  }
}
