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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import static org.slf4j.LoggerFactory.getLogger;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;

public class BackwardChain {
  private static final Logger LOG = getLogger(BackwardChain.class);

  private static final String FIRST_STORED_ANCESTOR_KEY = "firstStoredAncestor";
  private static final String LAST_STORED_PIVOT_KEY = "lastStoredPivot";

  private final GenericKeyValueStorageFacade<Hash, BlockHeader> headers;
  private final GenericKeyValueStorageFacade<Hash, Block> blocks;
  private final GenericKeyValueStorageFacade<Hash, Hash> chainStorage;
  private final GenericKeyValueStorageFacade<String, BlockHeader> sessionDataStorage;
  private Optional<BlockHeader> firstStoredAncestor;
  private Optional<BlockHeader> lastStoredPivot;
  private final Queue<Hash> hashesToAppend = new ArrayDeque<>();

  public BackwardChain(
      final GenericKeyValueStorageFacade<Hash, BlockHeader> headersStorage,
      final GenericKeyValueStorageFacade<Hash, Block> blocksStorage,
      final GenericKeyValueStorageFacade<Hash, Hash> chainStorage,
      final GenericKeyValueStorageFacade<String, BlockHeader> sessionDataStorage) {
    this.headers = headersStorage;
    this.blocks = blocksStorage;
    this.chainStorage = chainStorage;
    this.sessionDataStorage = sessionDataStorage;
    firstStoredAncestor =
        sessionDataStorage
            .get(FIRST_STORED_ANCESTOR_KEY)
            .map(
                header -> {
                  LOG.atDebug()
                      .setMessage(FIRST_STORED_ANCESTOR_KEY + " loaded from storage with value {}")
                      .addArgument(header::toLogString)
                      .log();
                  return header;
                });
    lastStoredPivot =
        sessionDataStorage
            .get(LAST_STORED_PIVOT_KEY)
            .map(
                header -> {
                  LOG.atDebug()
                      .setMessage(LAST_STORED_PIVOT_KEY + " loaded from storage with value {}")
                      .addArgument(header::toLogString)
                      .log();
                  return header;
                });
  }

  public static BackwardChain from(
      final StorageProvider storageProvider, final BlockHeaderFunctions blockHeaderFunctions) {
    return new BackwardChain(
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            BlocksHeadersConvertor.of(blockHeaderFunctions),
            storageProvider.getStorageBySegmentIdentifier(
                KeyValueSegmentIdentifier.BACKWARD_SYNC_HEADERS)),
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            BlocksConvertor.of(blockHeaderFunctions),
            storageProvider.getStorageBySegmentIdentifier(
                KeyValueSegmentIdentifier.BACKWARD_SYNC_BLOCKS)),
        new GenericKeyValueStorageFacade<>(
            hash -> hash.getBytes().toArrayUnsafe(),
            new HashConvertor(),
            storageProvider.getStorageBySegmentIdentifier(
                KeyValueSegmentIdentifier.BACKWARD_SYNC_CHAIN)),
        // using BACKWARD_SYNC_CHAIN that contains the sequence of the work to do,
        // to also store the session data that will be used to resume
        // the backward sync from where it was left before the restart
        new GenericKeyValueStorageFacade<>(
            key -> key.getBytes(StandardCharsets.UTF_8),
            BlocksHeadersConvertor.of(blockHeaderFunctions),
            storageProvider.getStorageBySegmentIdentifier(
                KeyValueSegmentIdentifier.BACKWARD_SYNC_CHAIN)));
  }

  public synchronized Optional<BlockHeader> getFirstAncestorHeader() {
    return firstStoredAncestor;
  }

  public synchronized List<BlockHeader> getFirstNAncestorHeaders(final int size) {
    List<BlockHeader> result = new ArrayList<>(size);
    Optional<BlockHeader> it = firstStoredAncestor;
    while (it.isPresent() && result.size() < size) {
      result.add(it.get());
      it = chainStorage.get(it.get().getHash()).flatMap(headers::get);
    }
    return result;
  }

  public synchronized void prependAncestorsHeader(final BlockHeader blockHeader) {
    headers.put(blockHeader.getHash(), blockHeader);

    if (firstStoredAncestor.isEmpty()) {
      updateLastStoredPivot(Optional.of(blockHeader));
    } else {
      final BlockHeader firstHeader = firstStoredAncestor.get();
      chainStorage.put(blockHeader.getHash(), firstHeader.getHash());
      LOG.atDebug()
          .setMessage("Added header {} to backward chain led by pivot {} on height {}")
          .addArgument(blockHeader::toLogString)
          .addArgument(() -> lastStoredPivot.orElseThrow().toLogString())
          .addArgument(firstHeader::getNumber)
          .log();
    }

    updateFirstStoredAncestor(Optional.of(blockHeader));
  }

  /**
   * Prepends a batch of headers already present in header storage (e.g. loaded from a previous
   * session) to the backward chain using a single chainStorage transaction, avoiding one
   * transaction per header. Headers must be supplied in parent-walk order (child first, then its
   * parent, and so on).
   *
   * @param restoredHeaders headers to link, each the parent of the previous entry (or of the
   *     current firstStoredAncestor for the first entry).
   */
  public synchronized void prependRestoredAncestorsHeaders(
      final List<BlockHeader> restoredHeaders) {
    if (restoredHeaders.isEmpty()) {
      return;
    }

    int firstIndex = 0;
    if (firstStoredAncestor.isEmpty()) {
      updateLastStoredPivot(Optional.of(restoredHeaders.getFirst()));
      updateFirstStoredAncestor(Optional.of(restoredHeaders.getFirst()));
      firstIndex = 1;
    }

    if (firstIndex < restoredHeaders.size()) {
      final Map<Hash, Hash> chainLinks = new LinkedHashMap<>(restoredHeaders.size() - firstIndex);
      Hash childHash = firstStoredAncestor.orElseThrow().getHash();
      for (final BlockHeader header : restoredHeaders.subList(firstIndex, restoredHeaders.size())) {
        chainLinks.put(header.getHash(), childHash);
        childHash = header.getHash();
      }
      chainStorage.putAll(chainLinks);
    }

    final BlockHeader lowestRestored = restoredHeaders.getLast();
    updateFirstStoredAncestor(Optional.of(lowestRestored));
    LOG.atDebug()
        .setMessage("Prepended batch of {} restored headers down to {}")
        .addArgument(restoredHeaders::size)
        .addArgument(lowestRestored::toLogString)
        .log();
  }

  private void updateFirstStoredAncestor(final Optional<BlockHeader> maybeHeader) {
    maybeHeader.ifPresentOrElse(
        header -> sessionDataStorage.put(FIRST_STORED_ANCESTOR_KEY, header),
        () -> sessionDataStorage.drop(FIRST_STORED_ANCESTOR_KEY));
    firstStoredAncestor = maybeHeader;
  }

  private void updateLastStoredPivot(final Optional<BlockHeader> maybeHeader) {
    maybeHeader.ifPresentOrElse(
        header -> sessionDataStorage.put(LAST_STORED_PIVOT_KEY, header),
        () -> sessionDataStorage.drop(LAST_STORED_PIVOT_KEY));
    lastStoredPivot = maybeHeader;
  }

  public synchronized Optional<Block> getPivot() {
    if (lastStoredPivot.isEmpty()) {
      return Optional.empty();
    }
    return blocks.get(lastStoredPivot.get().getHash());
  }

  public synchronized void dropFirstHeader() {
    if (firstStoredAncestor.isEmpty()) {
      return;
    }
    headers.drop(firstStoredAncestor.get().getHash());
    final Optional<Hash> hash = chainStorage.get(firstStoredAncestor.get().getHash());
    chainStorage.drop(firstStoredAncestor.get().getHash());
    updateFirstStoredAncestor(hash.flatMap(headers::get));
    if (firstStoredAncestor.isEmpty()) {
      updateLastStoredPivot(Optional.empty());
    }
  }

  public synchronized void appendTrustedBlock(final Block newPivot) {
    LOG.atDebug().setMessage("Appending trusted block {}").addArgument(newPivot::toLogString).log();
    headers.put(newPivot.getHash(), newPivot.getHeader());
    blocks.put(newPivot.getHash(), newPivot);
    if (lastStoredPivot.isEmpty()) {
      updateFirstStoredAncestor(Optional.of(newPivot.getHeader()));
    } else {
      if (newPivot.getHeader().getParentHash().equals(lastStoredPivot.get().getHash())) {
        LOG.atDebug()
            .setMessage("Added block {} to backward chain led by pivot {} on height {}")
            .addArgument(newPivot::toLogString)
            .addArgument(lastStoredPivot.get()::toLogString)
            .addArgument(firstStoredAncestor.get()::getNumber)
            .log();
        chainStorage.put(lastStoredPivot.get().getHash(), newPivot.getHash());
      } else {
        updateFirstStoredAncestor(Optional.of(newPivot.getHeader()));
        LOG.atDebug()
            .setMessage("Re-pivoting to new target block {}")
            .addArgument(newPivot::toLogString)
            .log();
      }
    }
    updateLastStoredPivot(Optional.of(newPivot.getHeader()));
  }

  public synchronized boolean isTrusted(final Hash hash) {
    return blocks.get(hash).isPresent();
  }

  public synchronized Block getTrustedBlock(final Hash hash) {
    return blocks.get(hash).orElseThrow();
  }

  public synchronized void clear() {
    blocks.clear();
    headers.clear();
    chainStorage.clear();
    sessionDataStorage.clear();
    firstStoredAncestor = Optional.empty();
    lastStoredPivot = Optional.empty();
    hashesToAppend.clear();
  }

  public synchronized Optional<Hash> getDescendant(final Hash blockHash) {
    return chainStorage.get(blockHash);
  }

  public synchronized Optional<Block> getBlock(final Hash hash) {
    return blocks.get(hash);
  }

  public synchronized Optional<BlockHeader> getHeader(final Hash hash) {
    return headers.get(hash);
  }

  public synchronized void addNewHash(final Hash newBlockHash) {
    if (hashesToAppend.contains(newBlockHash)) {
      return;
    }
    this.hashesToAppend.add(newBlockHash);
  }

  public synchronized Optional<Hash> getFirstHashToAppend() {
    return Optional.ofNullable(hashesToAppend.peek());
  }

  public synchronized void removeFromHashToAppend(final Hash hashToRemove) {
    if (hashesToAppend.contains(hashToRemove)) {
      hashesToAppend.remove(hashToRemove);
    }
  }

  @VisibleForTesting
  synchronized List<Hash> getHashesToAppend() {
    return new ArrayList<>(hashesToAppend);
  }
}
