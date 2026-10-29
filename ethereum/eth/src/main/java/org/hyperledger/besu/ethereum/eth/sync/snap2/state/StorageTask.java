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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Storage of an account that is not yet fully downloaded: the slot ranges already persisted, the
 * storage root responses are verified against, and the trie written from them.
 */
final class StorageTask {

  private final Bytes32 accountHash;
  private Bytes32 storageRoot;
  private boolean rootStale;
  private final KeyRangeSet covered = new KeyRangeSet();
  private long slots;
  // the trie of the ranges, given up once they come at several roots
  private Bytes32 trieRoot;
  private Bytes trieRootNode;
  private boolean trieGivenUp;

  // the slot ranges of its download, which only the loop of the state download touches
  final List<RangeTask> chunks = new ArrayList<>();

  StorageTask(final Bytes32 accountHash, final Bytes32 storageRoot) {
    // copies, as hashes decoded from a response slice the whole message
    this.accountHash = accountHash.copy();
    this.storageRoot = storageRoot.copy();
  }

  public Bytes32 accountHash() {
    return accountHash;
  }

  /** Returns the storage root at the current pivot, unless {@link #isRootStale()}. */
  public synchronized Bytes32 storageRoot() {
    return storageRoot;
  }

  /** Returns whether the storage changed since the root was learned, so it must be refreshed. */
  public synchronized boolean isRootStale() {
    return rootStale;
  }

  public synchronized void markRootStale() {
    rootStale = true;
  }

  /** Sets a root fetched at the current pivot, never one fetched before a pivot move. */
  public synchronized void refreshRoot(final Bytes32 root) {
    storageRoot = root.copy();
    rootStale = false;
  }

  /** Returns whether the slot is in a persisted range. */
  public boolean covers(final Bytes32 slotHash) {
    return covered.contains(slotHash);
  }

  /** What covering a slot range did. */
  enum CoverResult {
    /** The range was covered already, as a retried request's is, and counts nothing more. */
    ALREADY_COVERED,
    /** The range is new, and other ranges are still missing. */
    ADDED,
    /** The range is new and completes the storage, which only one call learns. */
    COMPLETED
  }

  /** Covers a slot range holding some slots. */
  public synchronized CoverResult cover(final Bytes32 from, final Bytes32 to, final int slots) {
    if (covered.encloses(from, to)) {
      return CoverResult.ALREADY_COVERED;
    }
    covered.add(from, to);
    this.slots += slots;
    return covered.isFull() ? CoverResult.COMPLETED : CoverResult.ADDED;
  }

  /** Returns the number of slots stored. */
  public synchronized long slots() {
    return slots;
  }

  /** Returns whether a range verified against this root goes into the trie of the first range. */
  public synchronized boolean writesTrieAt(final Bytes32 root) {
    if (trieRoot == null) {
      trieRoot = root.copy();
    } else if (!trieRoot.equals(root)) {
      trieGivenUp = true;
    }
    return !trieGivenUp;
  }

  /** Records the root node a range gives, or gives the trie up if it gives none. */
  public synchronized void trieWritten(final Optional<Bytes> rootNode) {
    if (rootNode.isEmpty()) {
      trieGivenUp = true;
    } else if (trieRootNode == null) {
      trieRootNode = rootNode.get().copy();
    }
  }

  /** Returns the root node of the trie, once every range came at the current, fresh root. */
  public synchronized Optional<Bytes> trieRootNode() {
    return trieGivenUp || rootStale || !storageRoot.equals(trieRoot)
        ? Optional.empty()
        : Optional.ofNullable(trieRootNode);
  }
}
