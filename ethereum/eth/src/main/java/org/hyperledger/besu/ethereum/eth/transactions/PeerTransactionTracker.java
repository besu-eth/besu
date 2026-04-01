/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.transactions;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNullElse;
import static org.hyperledger.besu.ethereum.core.Transaction.toHashList;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.plugin.data.AddedBlockContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.collections4.map.LRUMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerTransactionTracker
    implements EthPeer.DisconnectCallback,
        EthPeers.ConnectCallback,
        PendingTransactionDroppedListener,
        BlockAddedObserver {
  private static final Logger LOG = LoggerFactory.getLogger(PeerTransactionTracker.class);
  private final EthPeers ethPeers;
  private final EthScheduler ethScheduler;
  private final int maxSendQueueSizePerPeer;
  private final boolean forgetEvictedTxsEnabled;
  private final FixedCapacityLRUMap<Hash, PeersSeenState> peersSeenStateByHash;
  private final Map<EthPeer, SequencedSet<Transaction>> transactionsToSend = new HashMap<>();
  private final Map<EthPeer, SequencedSet<Transaction>> announcementsToSend = new HashMap<>();
  private final Map<EthPeer, LRUMap<Hash, TransactionAnnouncement>> announcementsToRequestByHash =
      new HashMap<>();
  private final Set<Hash> inProgressAnnouncements = new HashSet<>();
  private final BiMap<EthPeer, Integer> peerToSlotIndexMap;

  public PeerTransactionTracker(
      final TransactionPoolConfiguration txPoolConfig,
      final EthPeers ethPeers,
      final EthScheduler scheduler) {
    this.ethPeers = ethPeers;
    this.ethScheduler = scheduler;
    this.maxSendQueueSizePerPeer = txPoolConfig.getUnstable().getMaxSendQueueSizePerPeer();
    this.forgetEvictedTxsEnabled = txPoolConfig.getUnstable().getPeerTrackerForgetEvictedTxs();
    this.peersSeenStateByHash =
        new FixedCapacityLRUMap<>(txPoolConfig.getUnstable().getMaxTrackedSeenTxs());
    this.peerToSlotIndexMap = HashBiMap.create(ethPeers.getMaxPeers());
    ethScheduler.scheduleFutureTaskWithFixedDelay(
        this::logStats, Duration.ofMinutes(1), Duration.ofMinutes(1));
  }

  public synchronized void reset() {
    peerToSlotIndexMap.clear();
    peersSeenStateByHash.clear();
    transactionsToSend.clear();
    announcementsToSend.clear();
    announcementsToRequestByHash.clear();
    inProgressAnnouncements.clear();
  }

  private synchronized void logStats() {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Peers seen state by hash {}", peersSeenStateByHash.size());
      transactionsToSend.forEach(
          (ethPeer, txs) -> LOG.trace("Txs to send: peer={} size={}", ethPeer, txs.size()));
      announcementsToSend.forEach(
          (ethPeer, txs) ->
              LOG.trace("Announcements to send: peer={} size={}", ethPeer, txs.size()));
      announcementsToRequestByHash.forEach(
          (ethPeer, hashes) ->
              LOG.trace("Announcements to request: peer={} size={}", ethPeer, hashes.size()));
      LOG.trace("In progress announcements {}", inProgressAnnouncements.size());
    }
  }

  public synchronized void markTransactionsAsSeen(
      final EthPeer peer, final Collection<Hash> seenHashes) {
    if (!peer.isDisconnected()) {
      seenHashes.forEach(
          hash ->
              peersSeenStateByHash.compute(
                  hash,
                  (unused, peersSeenState) -> {
                    final PeersSeenState computed =
                        requireNonNullElse(
                            peersSeenState, new PeersSeenState(ethPeers.getMaxPeers()));
                    computed.transactionSeen(peerToSlotIndexMap.get(peer));
                    return computed;
                  }));
    }
    // remove the seen txs from any request queue
    removeAnnouncementsToRequest(seenHashes);
  }

  public synchronized void markAnnouncementsAsSeenByTransaction(
      final EthPeer peer, final Collection<Transaction> announcements) {
    if (!peer.isDisconnected()) {
      announcements.stream()
          .map(Transaction::getHash)
          .forEach(
              hash ->
                  peersSeenStateByHash.compute(
                      hash,
                      (unused, peersSeenState) -> {
                        final PeersSeenState computed =
                            requireNonNullElse(
                                peersSeenState, new PeersSeenState(ethPeers.getMaxPeers()));
                        computed.announcementSeen(peerToSlotIndexMap.get(peer));
                        return computed;
                      }));
    }
    // do not clean transactionAnnouncementsToRequest to allow for retries with other peers
  }

  public synchronized void markAnnouncementsAsSeen(
      final EthPeer peer, final Collection<TransactionAnnouncement> announcements) {
    if (!peer.isDisconnected()) {
      announcements.stream()
          .map(TransactionAnnouncement::hash)
          .forEach(
              hash ->
                  peersSeenStateByHash.compute(
                      hash,
                      (unused, peersSeenState) -> {
                        final PeersSeenState computed =
                            requireNonNullElse(
                                peersSeenState, new PeersSeenState(ethPeers.getMaxPeers()));
                        computed.announcementSeen(peerToSlotIndexMap.get(peer));
                        return computed;
                      }));
    }
    // do not clean transactionAnnouncementsToRequest to allow for retries with other peers
  }

  public synchronized void addToPeerSendQueue(
      final EthPeer peer, final List<Transaction> transactions) {
    if (!peer.isDisconnected()) {
      transactionsToSend
          .computeIfAbsent(
              peer, key -> createBoundedSet(transactions.size(), maxSendQueueSizePerPeer))
          .addAll(transactions);
    }
  }

  public synchronized void addToPeerAnnouncementsSendQueue(
      final EthPeer peer, final List<Transaction> transactions) {
    if (!peer.isDisconnected()) {
      announcementsToSend
          .computeIfAbsent(
              peer, key -> createBoundedSet(transactions.size(), maxSendQueueSizePerPeer))
          .addAll(transactions);
    }
  }

  public synchronized Transaction claimTransactionToSendToPeer(final EthPeer peer) {
    final SequencedSet<Transaction> txsToSend = transactionsToSend.get(peer);
    if (txsToSend != null && !txsToSend.isEmpty()) {
      final Transaction tx = txsToSend.removeFirst();
      if (txsToSend.isEmpty()) {
        transactionsToSend.remove(peer);
      }
      return tx;
    } else {
      return null;
    }
  }

  public synchronized Transaction claimAnnouncementToSendToPeer(final EthPeer peer) {
    final SequencedSet<Transaction> annsToSend = announcementsToSend.get(peer);
    if (annsToSend != null && !annsToSend.isEmpty()) {
      final Transaction announcement = annsToSend.removeFirst();
      if (annsToSend.isEmpty()) {
        announcementsToSend.remove(peer);
      }
      return announcement;
    } else {
      return null;
    }
  }

  @SuppressWarnings("MixedMutabilityReturnType")
  public synchronized List<TransactionAnnouncement> claimAnnouncementsToRequestFromPeer(
      final EthPeer peer, final int maxHashes, final long maxSize) {
    final LRUMap<Hash, TransactionAnnouncement> announcements =
        announcementsToRequestByHash.get(peer);

    if (announcements == null) {
      return emptyList();
    }

    final List<TransactionAnnouncement> returnAnnouncements = new ArrayList<>(maxHashes);
    final Iterator<TransactionAnnouncement> itAnnouncements = announcements.values().iterator();
    long currentSize = 0;
    while (returnAnnouncements.size() < maxHashes
        && currentSize < maxSize
        && itAnnouncements.hasNext()) {
      final TransactionAnnouncement announcement = itAnnouncements.next();

      if (alreadySeenTransaction(announcement.hash())) {
        itAnnouncements.remove();
      } else if (!inProgressAnnouncements.contains(announcement.hash())) {
        returnAnnouncements.add(announcement);
        inProgressAnnouncements.add(announcement.hash());
        currentSize += announcement.size();
        itAnnouncements.remove();
      }
      // if announcement is in progress, then keep in the queue, since it could be
      // tried later if the current in progress retrieval should fail
    }
    if (announcements.isEmpty()) {
      announcementsToRequestByHash.remove(peer);
    }
    return returnAnnouncements;
  }

  public synchronized void consumedAnnouncements(final List<Hash> requestedHashes) {
    requestedHashes.forEach(inProgressAnnouncements::remove);
  }

  public synchronized Collection<TransactionAnnouncement> receivedAnnouncements(
      final EthPeer peer, final List<TransactionAnnouncement> incomingAnnouncements) {

    if (peer.isDisconnected()) {
      return emptyList();
    }

    final List<TransactionAnnouncement> freshAnnouncements =
        incomingAnnouncements.stream()
            .filter(txAnnouncement -> !alreadySeenTransaction(txAnnouncement.hash()))
            .toList();

    if (!freshAnnouncements.isEmpty()) {
      final LRUMap<Hash, TransactionAnnouncement> announcementsByHashForPeer =
          announcementsToRequestByHash.computeIfAbsent(
              peer, key -> new LRUMap<>(maxSendQueueSizePerPeer, freshAnnouncements.size()));
      freshAnnouncements.forEach(ann -> announcementsByHashForPeer.put(ann.hash(), ann));
    }

    markAnnouncementsAsSeen(peer, incomingAnnouncements);

    return freshAnnouncements;
  }

  public synchronized Collection<Transaction> receivedTransactions(
      final EthPeer peer, final List<Transaction> incomingTransactions) {
    if (peer.isDisconnected()) {
      return emptyList();
    }

    final List<Transaction> freshTransactions =
        incomingTransactions.stream().filter(tx -> !alreadySeenTransaction(tx.getHash())).toList();

    markTransactionsAsSeen(peer, toHashList(incomingTransactions));

    return freshTransactions;
  }

  public synchronized boolean alreadySeenTransaction(final Hash txHash) {
    final PeersSeenState peersSeenState = peersSeenStateByHash.get(txHash);

    return (peersSeenState != null && peersSeenState.anyHasSeenTransaction());
  }

  public boolean hasPeerSeenTransaction(final EthPeer peer, final Transaction transaction) {
    return hasPeerSeenTransaction(peer, transaction.getHash());
  }

  public synchronized boolean hasPeerSeenTransaction(final EthPeer peer, final Hash txHash) {
    final PeersSeenState peersSeenState = peersSeenStateByHash.get(txHash);
    return peersSeenState != null
        && peersSeenState.hasSeenTransaction(peerToSlotIndexMap.get(peer));
  }

  public synchronized boolean hasPeerSeenAnnouncement(final EthPeer peer, final Hash txHash) {
    final PeersSeenState peersSeenState = peersSeenStateByHash.get(txHash);
    return peersSeenState != null
        && peersSeenState.hasSeenAnnouncement(peerToSlotIndexMap.get(peer));
  }

  public synchronized boolean hasPeerSeenTransactionOrAnnouncement(
      final EthPeer peer, final Hash txHash) {
    final PeersSeenState peersSeenState = peersSeenStateByHash.get(txHash);
    final Integer peerIdx = peerToSlotIndexMap.get(peer);

    return peersSeenState != null
        && (peersSeenState.hasSeenAnnouncement(peerIdx)
            || peersSeenState.hasSeenTransaction(peerIdx));
  }

  private <T> SequencedSet<T> createBoundedSet(final int initialCapacity, final int maxSize) {
    final int cappedInitialCapacity = Math.min(initialCapacity, maxSize);
    return Collections.newSequencedSetFromMap(
        new LinkedHashMap<>((int) Math.ceil(cappedInitialCapacity / 0.75f), 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(final Map.Entry<T, Boolean> eldest) {
            return size() > maxSize;
          }
        });
  }

  @Override
  public synchronized void onDisconnect(final EthPeer peer) {
    LOG.atTrace().setMessage("onDisconnect for peer {}").addArgument(peer::getLoggableId).log();

    // here we reconcile all the trackers with the active peers, since due to the asynchronous
    // processing of incoming messages it could seldom happen that a tracker is recreated just
    // after a peer was disconnected, resulting in a memory leak.
    final Set<EthPeer> trackedPeers = new HashSet<>(peerToSlotIndexMap.keySet());
    trackedPeers.addAll(transactionsToSend.keySet());
    trackedPeers.addAll(announcementsToSend.keySet());
    trackedPeers.addAll(announcementsToRequestByHash.keySet());

    LOG.atTrace()
        .setMessage("{} tracked peers ({})")
        .addArgument(trackedPeers.size())
        .addArgument(() -> logPeerSet(trackedPeers))
        .log();

    final Set<EthPeer> connectedPeers =
        ethPeers
            .streamAllPeers()
            .map(EthPeerImmutableAttributes::ethPeer)
            .collect(Collectors.toUnmodifiableSet());

    final Set<EthPeer> disconnectedPeers = trackedPeers;
    disconnectedPeers.removeAll(connectedPeers);
    LOG.atTrace()
        .setMessage("{} transaction trackers for disconnected peers ({}) will be removed")
        .addArgument(disconnectedPeers.size())
        .addArgument(() -> logPeerSet(disconnectedPeers))
        .log();

    disconnectedPeers.forEach(
        disconnectedPeer -> {
          transactionsToSend.remove(disconnectedPeer);
          announcementsToSend.remove(disconnectedPeer);
          announcementsToRequestByHash.remove(disconnectedPeer);
          final Integer removedPeerIndex = peerToSlotIndexMap.remove(disconnectedPeer);
          LOG.atTrace()
              .setMessage(
                  "onPeerDisconnected: removed transaction trackers for disconnected peer {} with index {} peersToSlotIndexMap {}")
              .addArgument(disconnectedPeer::getLoggableId)
              .addArgument(removedPeerIndex)
              .addArgument(this::logPeerToSlotIndexMap)
              .log();
        });
  }

  @Override
  public synchronized void onPeerConnected(final EthPeer newPeer) {
    final int firstFreeIndex =
        IntStream.range(0, peerToSlotIndexMap.size())
            .filter(idx -> !peerToSlotIndexMap.containsValue(idx))
            .findFirst()
            .orElse(peerToSlotIndexMap.size());
    peerToSlotIndexMap.put(newPeer, firstFreeIndex);

    // clear seen status for new peer index, just in case it was reused
    long start = System.nanoTime();
    peersSeenStateByHash.values().stream()
        .forEach(peersSeenState -> peersSeenState.clear(firstFreeIndex));

    LOG.atTrace()
        .setMessage(
            "onPeerConnected: new peer {} got index {}, its peersSeenState cleared in {} peersToSlotIndexMap {}")
        .addArgument(newPeer::getLoggableId)
        .addArgument(firstFreeIndex)
        .addArgument(() -> Duration.ofNanos(System.nanoTime() - start))
        .addArgument(this::logPeerToSlotIndexMap)
        .log();
  }

  private String logPeerSet(final Set<EthPeer> peers) {
    return peers.stream().map(EthPeer::getLoggableId).collect(Collectors.joining(","));
  }

  private String logPeerToSlotIndexMap() {
    return peerToSlotIndexMap.entrySet().stream()
        .map(e -> e.getKey().getLoggableId() + "->" + e.getValue())
        .collect(Collectors.joining(","));
  }

  @Override
  public synchronized void onTransactionDropped(
      final Transaction transaction, final RemovalReason reason) {
    if (reason.stopBroadcasting()) {
      final List<Transaction> droppedTxs = List.of(transaction);
      removeFromSendQueues(transactionsToSend, droppedTxs);
      removeFromSendQueues(announcementsToSend, droppedTxs);
      final List<Hash> droppedHashes = List.of(transaction.getHash());
      removeAnnouncementsToRequest(droppedHashes);
    }

    if (reason.stopTracking() && forgetEvictedTxsEnabled) {
      peersSeenStateByHash.remove(transaction.getHash());
    }
  }

  @Override
  public void onBlockAdded(final BlockAddedEvent event) {
    ethScheduler.scheduleServiceTask(
        () -> {
          if (event.getEventType().equals(AddedBlockContext.EventType.HEAD_ADVANCED)
              || event.getEventType().equals(AddedBlockContext.EventType.CHAIN_REORG)) {
            final List<Transaction> confirmedTxs = event.getBlock().getBody().getTransactions();
            final List<Hash> confirmedTxHashes = toHashList(confirmedTxs);

            synchronized (this) {
              confirmedTxHashes.forEach(
                  hash ->
                      peersSeenStateByHash.compute(
                          hash,
                          (unused, peersSeenState) -> {
                            final PeersSeenState computed =
                                requireNonNullElse(
                                    peersSeenState, new PeersSeenState(ethPeers.getMaxPeers()));
                            computed.transactionSeenAll();
                            return computed;
                          }));
              removeFromSendQueues(transactionsToSend, confirmedTxs);
              removeFromSendQueues(announcementsToSend, confirmedTxs);
              removeAnnouncementsToRequest(confirmedTxHashes);
            }
          }
        });
  }

  private void removeFromSendQueues(
      final Map<EthPeer, SequencedSet<Transaction>> sendQueues, final List<Transaction> removeTxs) {
    final Iterator<SequencedSet<Transaction>> itSendQueue = sendQueues.values().iterator();
    while (itSendQueue.hasNext()) {
      final SequencedSet<Transaction> sendQueue = itSendQueue.next();
      removeTxs.forEach(sendQueue::remove);
      if (sendQueue.isEmpty()) {
        itSendQueue.remove();
      }
    }
  }

  private void removeAnnouncementsToRequest(final Collection<Hash> seenHashes) {
    final Iterator<LRUMap<Hash, TransactionAnnouncement>> itAnnReqs =
        announcementsToRequestByHash.values().iterator();
    while (itAnnReqs.hasNext()) {
      final LRUMap<Hash, TransactionAnnouncement> annReqs = itAnnReqs.next();
      seenHashes.forEach(annReqs::remove);
      if (annReqs.isEmpty()) {
        itAnnReqs.remove();
      }
    }
  }

  private record PeersSeenState(BitSet transactions, BitSet announcements) {
    PeersSeenState(final int maxSlots) {
      this(new BitSet(maxSlots), new BitSet(maxSlots));
    }

    public void transactionSeenAll() {
      transactions.set(0, transactions.size());
    }

    public void transactionSeen(final Integer peerIdx) {
      if (peerIdx != null) {
        transactions.set(peerIdx);
      }
    }

    public void announcementSeen(final Integer peerIdx) {
      if (peerIdx != null) {
        announcements.set(peerIdx);
      }
    }

    public boolean anyHasSeenTransaction() {
      return !transactions.isEmpty();
    }

    public boolean hasSeenTransaction(final Integer peerIdx) {
      return peerIdx != null && transactions.get(peerIdx);
    }

    public boolean hasSeenAnnouncement(final Integer peerIdx) {
      return peerIdx != null && announcements.get(peerIdx);
    }

    public void clear(final Integer peerIdx) {
      transactions.clear(peerIdx);
      announcements.clear(peerIdx);
    }
  }

  /**
   * An LRU map with a strictly fixed capacity. The parent {@link LRUMap} grows beyond its declared
   * maximum when the internal hash table needs to be resized (because the number of buckets is
   * calculated from {@code maxEntries * loadFactor}). This subclass prevents that by:
   *
   * <ul>
   *   <li>Overriding {@link #calculateNewCapacity} to pre-size the backing array to exactly {@code
   *       proposedCapacity / loadFactor} buckets so no rehash is ever triggered.
   *   <li>Making {@link #checkCapacity} a no-op, because the map will never grow past {@code
   *       maxEntries} entries and a capacity check would be misleading.
   * </ul>
   *
   * <p>The {@link #DEFAULT_LOAD_FACTOR load factor} of {@code 0.8} keeps hash-collision probability
   * low while limiting memory overhead to ~25 % over the theoretical minimum.
   */
  private static class FixedCapacityLRUMap<K, V> extends LRUMap<K, V> {
    private static final float DEFAULT_LOAD_FACTOR = 0.8f;

    public FixedCapacityLRUMap(final int maxEntries) {
      super(maxEntries, DEFAULT_LOAD_FACTOR);
    }

    @Override
    protected int calculateNewCapacity(final int proposedCapacity) {
      return (int) Math.ceil(proposedCapacity / DEFAULT_LOAD_FACTOR);
    }

    @Override
    protected void checkCapacity() {
      // no-op since the fixed map never needs to resize
    }
  }
}
