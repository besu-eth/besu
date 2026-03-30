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

import static java.util.Collections.unmodifiableList;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult.ADDED;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult.ALREADY_KNOWN;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult.REJECTED_UNDERPRICED_REPLACEMENT;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult.TRY_NEXT_LAYER;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolStructuredLogUtils.logAdded;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolStructuredLogUtils.logMoved;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolStructuredLogUtils.logPenalized;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolStructuredLogUtils.logRemoved;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.AddReason.MOVE;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredRemovalReason.LayerMoveReason.EVICTED;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredRemovalReason.PoolRemovalReason.BELOW_MIN_SCORE;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredRemovalReason.PoolRemovalReason.CONFIRMED;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredRemovalReason.PoolRemovalReason.CROSS_LAYER_REPLACED;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredRemovalReason.PoolRemovalReason.REPLACED;
import static org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredRemovalReason.RemovedFrom.POOL;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactionAddedListener;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactionDroppedListener;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolMetrics;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;
import org.hyperledger.besu.util.Subscribers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTransactionsLayer implements TransactionsLayer {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractTransactionsLayer.class);
  private static final NavigableMap<Long, PendingTransaction> EMPTY_SENDER_TXS = new TreeMap<>();
  private static final int[] UNLIMITED_PROMOTIONS_PER_TYPE =
      new int[TransactionType.values().length];

  static {
    Arrays.fill(UNLIMITED_PROMOTIONS_PER_TYPE, Integer.MAX_VALUE);
  }

  protected final TransactionPoolConfiguration poolConfig;
  protected final TransactionsLayer nextLayer;
  protected final BiFunction<PendingTransaction, PendingTransaction, Boolean>
      transactionReplacementTester;
  protected final TransactionPoolMetrics metrics;
  protected final Map<Hash, PendingTransaction> pendingTransactions = new HashMap<>();
  protected final Map<Address, NavigableMap<Long, PendingTransaction>> txsBySender =
      new HashMap<>();
  private final Subscribers<PendingTransactionAddedListener> onAddedListeners =
      Subscribers.create();
  private final Subscribers<PendingTransactionDroppedListener> onDroppedListeners =
      Subscribers.create();
  private OptionalLong nextLayerOnAddedListenerId = OptionalLong.empty();
  private OptionalLong nextLayerOnDroppedListenerId = OptionalLong.empty();
  protected long spaceUsed = 0;
  protected final int[] txCountByType = new int[TransactionType.values().length];
  private final BlobCache blobCache;
  private final EthScheduler ethScheduler;

  protected AbstractTransactionsLayer(
      final TransactionPoolConfiguration poolConfig,
      final EthScheduler ethScheduler,
      final TransactionsLayer nextLayer,
      final BiFunction<PendingTransaction, PendingTransaction, Boolean>
          transactionReplacementTester,
      final TransactionPoolMetrics metrics,
      final BlobCache blobCache) {
    this.poolConfig = poolConfig;
    this.ethScheduler = ethScheduler;
    this.nextLayer = nextLayer;
    this.transactionReplacementTester = transactionReplacementTester;
    this.metrics = metrics;
    metrics.initSpaceUsed(this::getLayerSpaceUsed, name());
    metrics.initTransactionCount(pendingTransactions::size, name());
    metrics.initUniqueSenderCount(txsBySender::size, name());
    Arrays.stream(TransactionType.values())
        .forEach(
            type ->
                metrics.initTransactionCountByType(
                    () -> txCountByType[type.ordinal()], name(), type));
    this.blobCache = blobCache;
  }

  protected abstract boolean gapsAllowed();

  @Override
  public void reset() {
    pendingTransactions.clear();
    txsBySender.clear();
    spaceUsed = 0;
    Arrays.fill(txCountByType, 0);
    nextLayer.reset();
  }

  @Override
  public Optional<PendingTransaction> getByHash(final Hash transactionHash) {
    final var currLayerTx = pendingTransactions.get(transactionHash);
    if (currLayerTx == null) {
      return nextLayer.getByHash(transactionHash);
    }
    return Optional.of(currLayerTx);
  }

  @Override
  public boolean contains(final Transaction transaction) {
    return pendingTransactions.containsKey(transaction.getHash())
        || nextLayer.contains(transaction);
  }

  /**
   * Return the full content of this layer, organized as a list of sender pending txs. For each
   * sender the collection pending txs is ordered by nonce asc.
   *
   * @return a list of sender pending txs
   */
  @VisibleForTesting
  abstract List<List<PendingTransaction>> getBySender();

  @Override
  public List<PendingTransaction> getAll() {
    final List<PendingTransaction> allNextLayers = nextLayer.getAll();
    final List<PendingTransaction> allTxs =
        new ArrayList<>(pendingTransactions.size() + allNextLayers.size());
    allTxs.addAll(pendingTransactions.values());
    allTxs.addAll(allNextLayers);
    return allTxs;
  }

  @Override
  public long getCumulativeUsedSpace() {
    return getLayerSpaceUsed() + nextLayer.getCumulativeUsedSpace();
  }

  protected long getLayerSpaceUsed() {
    return spaceUsed;
  }

  protected abstract TransactionAddedResult canAdd(
      final PendingTransaction pendingTransaction, final int gap);

  @Override
  public TransactionAddedResult add(
      final PendingTransaction pendingTransaction, final int gap, final AddReason addReason) {

    // is replacing an existing one?
    TransactionAddedResult addStatus = maybeReplaceTransaction(pendingTransaction);
    if (addStatus == null) {
      addStatus = canAdd(pendingTransaction, gap);
    }

    if (addStatus.equals(TRY_NEXT_LAYER)) {
      return addToNextLayer(pendingTransaction, gap, addReason);
    }

    if (addStatus.isSuccess()) {
      final var addedPendingTransaction =
          addReason.makeCopy() ? pendingTransaction.detachedCopy() : pendingTransaction;
      processAdded(addedPendingTransaction, addReason);
      addStatus.maybeReplacedTransaction().ifPresent(this::replaced);

      nextLayer.notifyAdded(addedPendingTransaction);

      if (!maybeFull()) {
        // if there is space try to see if the added tx filled some gaps
        tryFillGap(addStatus, addedPendingTransaction, getRemainingPromotionsPerType());
      }

      if (addReason.sendNotification()) {
        logAdded(addedPendingTransaction, gap, addReason, name());
        ethScheduler.scheduleServiceTask(() -> notifyTransactionAdded(addedPendingTransaction));
      }

    } else {
      final var rejectReason = addStatus.maybeInvalidReason().orElseThrow();
      metrics.incrementRejected(pendingTransaction, rejectReason, name());
      LOG.atTrace()
          .setMessage("Transaction {} rejected reason {}")
          .addArgument(pendingTransaction::toTraceLog)
          .addArgument(rejectReason)
          .log();
    }

    return addStatus;
  }

  private boolean maybeFull() {
    final long cacheFreeSpace = cacheFreeSpace();
    final int overflowTxsCount = pendingTransactions.size() - maxTransactionsNumber();
    if (cacheFreeSpace < 0 || overflowTxsCount > 0) {
      LOG.atDebug()
          .setMessage("Layer full: {}")
          .addArgument(
              () ->
                  cacheFreeSpace < 0
                      ? "need to free " + (-cacheFreeSpace) + " space"
                      : "need to evict " + overflowTxsCount + " transaction(s)")
          .log();

      evict(-cacheFreeSpace, overflowTxsCount);
      return true;
    }
    return false;
  }

  private void tryFillGap(
      final TransactionAddedResult addStatus,
      final PendingTransaction pendingTransaction,
      final int[] remainingPromotionsPerType) {
    // it makes sense to fill gaps only if the add is not a replacement and this layer does not
    // allow gaps
    if (!addStatus.isReplacement() && !gapsAllowed()) {
      final PendingTransaction promotedTx =
          nextLayer.promoteFor(
              pendingTransaction.getSender(),
              pendingTransaction.getNonce(),
              remainingPromotionsPerType);
      if (promotedTx != null) {
        processAdded(promotedTx, AddReason.PROMOTED);
        if (!maybeFull()) {
          tryFillGap(ADDED, promotedTx, remainingPromotionsPerType);
        }
      }
    }
  }

  @Override
  public void notifyAdded(final PendingTransaction pendingTransaction) {
    final Address sender = pendingTransaction.getSender();
    final var senderTxs = txsBySender.get(sender);
    if (senderTxs != null) {
      if (senderTxs.firstKey() < pendingTransaction.getNonce()) {
        // in the case the world state has been updated but the confirmed txs have not yet been
        // processed
        confirmed(sender, pendingTransaction.getNonce());
      } else if (senderTxs.firstKey() == pendingTransaction.getNonce()) {
        // it is a cross layer replacement, namely added to a previous layer
        final PendingTransaction replacedTx = senderTxs.pollFirstEntry().getValue();
        processRemove(senderTxs, replacedTx.getTransaction(), CROSS_LAYER_REPLACED);

        if (senderTxs.isEmpty()) {
          txsBySender.remove(sender);
        }
      } else {
        internalNotifyAdded(senderTxs, pendingTransaction);
      }
    }
    nextLayer.notifyAdded(pendingTransaction);
  }

  protected abstract void internalNotifyAdded(
      final NavigableMap<Long, PendingTransaction> senderTxs,
      final PendingTransaction pendingTransaction);

  @Override
  public PendingTransaction promoteFor(
      final Address sender, final long nonce, final int[] remainingPromotionsPerType) {
    final var senderTxs = txsBySender.get(sender);
    if (senderTxs != null) {
      long expectedNonce = nonce + 1;
      if (senderTxs.firstKey() == expectedNonce) {
        final var candidateTx = senderTxs.firstEntry().getValue();
        final var txType = candidateTx.getTransaction().getType();

        if (remainingPromotionsPerType[txType.ordinal()] > 0) {
          senderTxs.pollFirstEntry();
          processRemove(
              senderTxs,
              candidateTx.getTransaction(),
              LayeredRemovalReason.LayerMoveReason.PROMOTED);
          metrics.incrementRemoved(candidateTx, "promoted", name());

          if (senderTxs.isEmpty()) {
            txsBySender.remove(sender);
          }
          --remainingPromotionsPerType[txType.ordinal()];
          return candidateTx;
        }
        return null;
      }
    }
    return nextLayer.promoteFor(sender, nonce, remainingPromotionsPerType);
  }

  private TransactionAddedResult addToNextLayer(
      final PendingTransaction pendingTransaction, final int distance, final AddReason addReason) {
    return addToNextLayer(
        txsBySender.getOrDefault(pendingTransaction.getSender(), EMPTY_SENDER_TXS),
        pendingTransaction,
        distance,
        addReason);
  }

  protected TransactionAddedResult addToNextLayer(
      final NavigableMap<Long, PendingTransaction> senderTxs,
      final PendingTransaction pendingTransaction,
      final int distance,
      final AddReason addReason) {
    final int nextLayerDistance;
    if (senderTxs.isEmpty()) {
      nextLayerDistance = distance;
    } else {
      nextLayerDistance = (int) (pendingTransaction.getNonce() - (senderTxs.lastKey() + 1));
    }
    return nextLayer.add(pendingTransaction, nextLayerDistance, addReason);
  }

  private void processAdded(final PendingTransaction addedTx, final AddReason addReason) {
    pendingTransactions.put(addedTx.getHash(), addedTx);
    final var senderTxs = txsBySender.computeIfAbsent(addedTx.getSender(), s -> new TreeMap<>());
    senderTxs.put(addedTx.getNonce(), addedTx);
    increaseCounters(addedTx);
    metrics.incrementAdded(addedTx, addReason, name());
    internalAdd(senderTxs, addedTx);
    LOG.atTrace()
        .setMessage("[{}] added pending transactions {}, reason: {}")
        .addArgument(name())
        .addArgument(addedTx::toTraceLog)
        .addArgument(addReason)
        .log();
  }

  protected abstract void internalAdd(
      final NavigableMap<Long, PendingTransaction> senderTxs, final PendingTransaction addedTx);

  protected abstract int maxTransactionsNumber();

  private void evict(final long spaceToFree, final int txsToEvict) {
    final var evictableTx = getEvictable();
    if (evictableTx != null) {
      final var lessReadySender = evictableTx.getSender();
      final var lessReadySenderTxs = txsBySender.get(lessReadySender);

      long evictedSize = 0;
      int evictedCount = 0;
      PendingTransaction lastTx;
      // lastTx must never be null, because the sender have at least the lessReadyTx
      while ((evictedSize < spaceToFree || txsToEvict > evictedCount)
          && !lessReadySenderTxs.isEmpty()) {
        lastTx = lessReadySenderTxs.pollLastEntry().getValue();
        processEvict(lessReadySenderTxs, lastTx, EVICTED);
        ++evictedCount;
        evictedSize += lastTx.memorySize();
        // evicted can always be added to the next layer
        addToNextLayer(lessReadySenderTxs, lastTx, 0, MOVE);
      }

      if (lessReadySenderTxs.isEmpty()) {
        txsBySender.remove(lessReadySender);
      }

      final long newSpaceToFree = spaceToFree - evictedSize;
      final int newTxsToEvict = txsToEvict - evictedCount;

      if ((newSpaceToFree > 0 || newTxsToEvict > 0) && !txsBySender.isEmpty()) {
        // try next less valuable sender
        evict(newSpaceToFree, newTxsToEvict);
      }
    }
  }

  protected void replaced(final PendingTransaction replacedTx) {
    pendingTransactions.remove(replacedTx.getHash());
    decreaseCounters(replacedTx);
    metrics.incrementRemoved(replacedTx, REPLACED.label(), name());
    internalReplaced(replacedTx);
    notifyTransactionDropped(replacedTx, REPLACED);
  }

  protected abstract void internalReplaced(final PendingTransaction replacedTx);

  private TransactionAddedResult maybeReplaceTransaction(final PendingTransaction incomingTx) {

    final var existingTxs = txsBySender.get(incomingTx.getSender());

    if (existingTxs != null) {
      final var existingReadyTx = existingTxs.get(incomingTx.getNonce());
      if (existingReadyTx != null) {

        if (existingReadyTx.getHash().equals(incomingTx.getHash())) {
          return ALREADY_KNOWN;
        }

        if (!transactionReplacementTester.apply(existingReadyTx, incomingTx)) {
          return REJECTED_UNDERPRICED_REPLACEMENT;
        }
        return TransactionAddedResult.createForReplacement(existingReadyTx);
      }
    }
    return null;
  }

  protected PendingTransaction processRemove(
      final NavigableMap<Long, PendingTransaction> senderTxs,
      final Transaction transaction,
      final LayeredRemovalReason removalReason) {
    final PendingTransaction removedTx = pendingTransactions.remove(transaction.getHash());

    if (removedTx != null) {
      decreaseCounters(removedTx);
      metrics.incrementRemoved(removedTx, removalReason.label(), name());
      internalRemove(senderTxs, removedTx, removalReason);
      if (removalReason.removedFrom().equals(POOL)) {
        logRemoved(removedTx, removalReason, name());
        notifyTransactionDropped(removedTx, removalReason);
      } else {
        logMoved(removedTx, removalReason, name());
      }
    }
    return removedTx;
  }

  protected PendingTransaction processEvict(
      final NavigableMap<Long, PendingTransaction> senderTxs,
      final PendingTransaction evictedTx,
      final LayeredRemovalReason reason) {
    final PendingTransaction removedTx = pendingTransactions.remove(evictedTx.getHash());
    if (removedTx != null) {
      decreaseCounters(evictedTx);
      metrics.incrementRemoved(evictedTx, reason.label(), name());
      internalEvict(senderTxs, removedTx);
    }
    return removedTx;
  }

  protected abstract void internalEvict(
      final NavigableMap<Long, PendingTransaction> lessReadySenderTxs,
      final PendingTransaction evictedTx);

  @Override
  public final void blockAdded(
      final FeeMarket feeMarket,
      final BlockHeader blockHeader,
      final Map<Address, Long> maxConfirmedNonceBySender) {
    LOG.atDebug()
        .setMessage("Managing new added block {}")
        .addArgument(blockHeader::toLogString)
        .log();

    nextLayer.blockAdded(feeMarket, blockHeader, maxConfirmedNonceBySender);
    maxConfirmedNonceBySender.forEach(this::confirmed);
    internalBlockAdded(blockHeader, feeMarket);
    promoteTransactions();
  }

  protected abstract void internalBlockAdded(
      final BlockHeader blockHeader, final FeeMarket feeMarket);

  final void promoteTransactions() {
    final int freeSlots = maxTransactionsNumber() - pendingTransactions.size();
    final long freeSpace = cacheFreeSpace();

    if (freeSlots > 0 && freeSpace > 0) {
      nextLayer
          .promote(
              this::promotionFilter, cacheFreeSpace(), freeSlots, getRemainingPromotionsPerType())
          .forEach(addedTx -> processAdded(addedTx, AddReason.PROMOTED));
    }
  }

  @Override
  public void penalize(
      final PendingTransaction penalizedTransaction,
      final TransactionSelectionResult selectionResult) {
    if (pendingTransactions.containsKey(penalizedTransaction.getHash())) {
      internalPenalize(penalizedTransaction);
      metrics.incrementPenalized(penalizedTransaction, name());
      logPenalized(penalizedTransaction, selectionResult, name());
      if (penalizedTransaction.getScore() < poolConfig.getMinScore()) {
        remove(penalizedTransaction, BELOW_MIN_SCORE);
      }
    } else {
      nextLayer.penalize(penalizedTransaction, selectionResult);
    }
  }

  protected abstract void internalPenalize(final PendingTransaction pendingTransaction);

  /**
   * How many txs of a specified type can be promoted? This make sense when a max number of txs of a
   * type can be included in a single block (ex. blob txs), to avoid filling the layer with more txs
   * than the useful ones. By default, there are no limits, but each layer can define its own
   * policy.
   *
   * @return an array containing the max amount of txs that can be promoted for each type
   */
  protected int[] getRemainingPromotionsPerType() {
    return Arrays.copyOf(UNLIMITED_PROMOTIONS_PER_TYPE, UNLIMITED_PROMOTIONS_PER_TYPE.length);
  }

  private void confirmed(final Address sender, final long maxConfirmedNonce) {
    final var senderTxs = txsBySender.get(sender);

    if (senderTxs != null) {
      final var confirmedTxs = senderTxs.headMap(maxConfirmedNonce, true);
      final var highestNonceRemovedTx =
          confirmedTxs.isEmpty() ? null : confirmedTxs.lastEntry().getValue();

      final var itConfirmedTxs = confirmedTxs.values().iterator();
      while (itConfirmedTxs.hasNext()) {
        final var confirmedTx = itConfirmedTxs.next();
        itConfirmedTxs.remove();
        if (confirmedTx.getTransaction().getBlobsWithCommitments().isPresent()) {
          this.blobCache.cacheBlobs(confirmedTx.getTransaction());
        }
        processRemove(senderTxs, confirmedTx.getTransaction(), CONFIRMED);

        metrics.incrementRemoved(confirmedTx, "confirmed", name());
        LOG.atTrace()
            .setMessage("Removed confirmed pending transactions {}")
            .addArgument(confirmedTx::toTraceLog)
            .log();
      }

      if (senderTxs.isEmpty()) {
        txsBySender.remove(sender);
      } else {
        internalConfirmed(senderTxs, sender, maxConfirmedNonce, highestNonceRemovedTx);
      }
    }
  }

  protected abstract void internalConfirmed(
      final NavigableMap<Long, PendingTransaction> senderTxs,
      final Address sender,
      final long maxConfirmedNonce,
      final PendingTransaction highestNonceRemovedTx);

  protected abstract void internalRemove(
      final NavigableMap<Long, PendingTransaction> senderTxs,
      final PendingTransaction pendingTransaction,
      final LayeredRemovalReason removalReason);

  protected abstract PendingTransaction getEvictable();

  protected void increaseCounters(final PendingTransaction pendingTransaction) {
    spaceUsed += pendingTransaction.memorySize();
    ++txCountByType[pendingTransaction.getTransaction().getType().ordinal()];
  }

  protected void decreaseCounters(final PendingTransaction pendingTransaction) {
    spaceUsed -= pendingTransaction.memorySize();
    --txCountByType[pendingTransaction.getTransaction().getType().ordinal()];
  }

  protected abstract long cacheFreeSpace();

  protected abstract boolean promotionFilter(PendingTransaction pendingTransaction);

  @Override
  public List<Transaction> getAllLocal() {
    final var localTxs =
        pendingTransactions.values().stream()
            .filter(PendingTransaction::isReceivedFromLocalSource)
            .map(PendingTransaction::getTransaction)
            .collect(Collectors.toCollection(ArrayList::new));
    localTxs.addAll(nextLayer.getAllLocal());
    return localTxs;
  }

  @Override
  public List<Transaction> getAllPriority() {
    final var priorityTxs =
        pendingTransactions.values().stream()
            .filter(PendingTransaction::hasPriority)
            .map(PendingTransaction::getTransaction)
            .collect(Collectors.toCollection(ArrayList::new));
    priorityTxs.addAll(nextLayer.getAllPriority());
    return priorityTxs;
  }

  @Override
  public synchronized List<PendingTransaction> getAllFor(final Address sender) {
    final var fromNextLayers = nextLayer.getAllFor(sender);
    final var fromThisLayer = txsBySender.getOrDefault(sender, EMPTY_SENDER_TXS).values();
    final var concatLayers =
        new ArrayList<PendingTransaction>(fromThisLayer.size() + fromNextLayers.size());
    concatLayers.addAll(fromThisLayer);
    concatLayers.addAll(fromNextLayers);
    return unmodifiableList(concatLayers);
  }

  @Override
  public int count() {
    return pendingTransactions.size() + nextLayer.count();
  }

  protected void notifyTransactionAdded(final PendingTransaction pendingTransaction) {
    onAddedListeners.forEach(
        listener -> listener.onTransactionAdded(pendingTransaction.getTransaction()));
  }

  protected void notifyTransactionDropped(
      final PendingTransaction pendingTransaction, final LayeredRemovalReason reason) {
    onDroppedListeners.forEach(
        listener -> listener.onTransactionDropped(pendingTransaction.getTransaction(), reason));
  }

  @Override
  public long subscribeToAdded(final PendingTransactionAddedListener listener) {
    nextLayerOnAddedListenerId = OptionalLong.of(nextLayer.subscribeToAdded(listener));
    return onAddedListeners.subscribe(listener);
  }

  @Override
  public void unsubscribeFromAdded(final long id) {
    nextLayerOnAddedListenerId.ifPresent(nextLayer::unsubscribeFromAdded);
    onAddedListeners.unsubscribe(id);
  }

  @Override
  public long subscribeToDropped(final PendingTransactionDroppedListener listener) {
    nextLayerOnDroppedListenerId = OptionalLong.of(nextLayer.subscribeToDropped(listener));
    return onDroppedListeners.subscribe(listener);
  }

  @Override
  public void unsubscribeFromDropped(final long id) {
    nextLayerOnDroppedListenerId.ifPresent(nextLayer::unsubscribeFromDropped);
    onDroppedListeners.unsubscribe(id);
  }

  @Override
  public String logStats() {
    return internalLogStats() + " | " + nextLayer.logStats();
  }

  @Override
  public String logSender(final Address sender) {
    final var senderTxs = txsBySender.get(sender);
    return name()
        + "["
        + (Objects.isNull(senderTxs) ? "Empty" : senderTxs.keySet())
        + "] "
        + nextLayer.logSender(sender);
  }

  protected abstract String internalLogStats();

  boolean consistencyCheck(
      final Map<Address, NavigableMap<Long, PendingTransaction>> prevLayerTxsBySender) {
    final BinaryOperator<PendingTransaction> noMergeExpected =
        (a, b) -> {
          throw new IllegalArgumentException();
        };
    final var controlTxsBySender =
        pendingTransactions.values().stream()
            .collect(
                Collectors.groupingBy(
                    PendingTransaction::getSender,
                    Collectors.toMap(
                        PendingTransaction::getNonce,
                        Function.identity(),
                        noMergeExpected,
                        TreeMap::new)));

    assert txsBySender.equals(controlTxsBySender)
        : "pendingTransactions and txsBySender do not contain the same txs";

    assert pendingTransactions.values().stream().mapToInt(PendingTransaction::memorySize).sum()
            == spaceUsed
        : "space used does not match";

    internalConsistencyCheck(prevLayerTxsBySender);

    if (nextLayer instanceof AbstractTransactionsLayer) {
      txsBySender.forEach(
          (sender, txsByNonce) ->
              prevLayerTxsBySender
                  .computeIfAbsent(sender, s -> new TreeMap<>())
                  .putAll(txsByNonce));
      return ((AbstractTransactionsLayer) nextLayer).consistencyCheck(prevLayerTxsBySender);
    }
    return true;
  }

  protected abstract void internalConsistencyCheck(
      final Map<Address, NavigableMap<Long, PendingTransaction>> prevLayerTxsBySender);

  public BlobCache getBlobCache() {
    return blobCache;
  }
}
