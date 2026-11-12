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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * The head of the latest fork choice and the ancestors of it found so far. Only fork choice may
 * move the chain head, so backward sync moves it only to these blocks.
 */
class ForkChoicePath {
  private final Function<Hash, Optional<BlockHeader>> headerLookup;
  private volatile Hash forkChoiceHead;

  private BlockHeader pathHead;
  private BlockHeader lowestOnPath;
  private final Set<Hash> pathHashes = new HashSet<>();

  ForkChoicePath(final Function<Hash, Optional<BlockHeader>> headerLookup) {
    this.headerLookup = headerLookup;
  }

  void setForkChoiceHead(final Hash headHash) {
    forkChoiceHead = headHash;
  }

  /**
   * Whether the block is the head of the latest fork choice or one of its ancestors.
   *
   * @param header the header of the block
   * @return true if fork choice selected the block
   */
  synchronized boolean contains(final BlockHeader header) {
    final Hash head = forkChoiceHead;
    if (head == null) {
      return false;
    }
    if (pathHead == null || !pathHead.getHash().equals(head)) {
      final Optional<BlockHeader> headHeader = headerLookup.apply(head);
      if (headHeader.isEmpty()) {
        return false;
      }
      moveHead(headHeader.get());
    }
    // the session downloads the headers while it runs, so the walk resumes where it stopped
    while (lowestOnPath.getNumber() > header.getNumber()) {
      final Optional<BlockHeader> parent = headerLookup.apply(lowestOnPath.getParentHash());
      if (parent.isEmpty()) {
        break;
      }
      lowestOnPath = parent.get();
      pathHashes.add(lowestOnPath.getHash());
    }
    return pathHashes.contains(header.getHash());
  }

  /** Forgets the walked ancestors, keeping the fork choice head. */
  synchronized void reset() {
    pathHead = null;
    lowestOnPath = null;
    pathHashes.clear();
  }

  private void moveHead(final BlockHeader newHead) {
    // the next fork choice usually builds on the previous one, so its walked ancestors are kept
    final List<Hash> newHashes = new ArrayList<>();
    newHashes.add(newHead.getHash());
    BlockHeader current = newHead;
    if (pathHead != null) {
      while (current.getNumber() > pathHead.getNumber()) {
        final Optional<BlockHeader> parent = headerLookup.apply(current.getParentHash());
        if (parent.isEmpty()) {
          break;
        }
        current = parent.get();
        newHashes.add(current.getHash());
      }
    }
    if (pathHead == null || !current.getHash().equals(pathHead.getHash())) {
      pathHashes.clear();
      lowestOnPath = current;
    }
    pathHashes.addAll(newHashes);
    pathHead = newHead;
  }
}
