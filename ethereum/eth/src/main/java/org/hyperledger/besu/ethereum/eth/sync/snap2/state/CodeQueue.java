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

import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The bytecodes to download: each waits in the queue once, and goes back to it when the request
 * that asked for it did not store it. Not thread safe: the loop of the state download owns it.
 */
final class CodeQueue {

  private static final Logger LOG = LoggerFactory.getLogger(CodeQueue.class);
  static final int SEEN_LIMIT = 200_000;

  private final SyncStateStorage storage;
  private final LinkedHashSet<Bytes32> queued = new LinkedHashSet<>();
  private final Set<Bytes32> inFlight = new HashSet<>();
  // only spares the lookups of common bytecodes, it is bounded and may forget
  private final Set<Bytes32> seen = new HashSet<>();

  CodeQueue(final SyncStateStorage storage) {
    this.storage = storage;
  }

  /** Queues a bytecode, unless it was queued lately or is in flight. */
  void add(final Bytes32 codeHash) {
    if (seen.contains(codeHash) || inFlight.contains(codeHash)) {
      return;
    }
    if (seen.size() >= SEEN_LIMIT) {
      seen.clear();
    }
    // a copy, as hashes decoded from a response slice the whole message
    final Bytes32 copy = codeHash.copy();
    seen.add(copy);
    queued.add(copy);
  }

  boolean isEmpty() {
    return queued.isEmpty();
  }

  int size() {
    return queued.size();
  }

  /** Takes the next codes not stored yet, {@code max} at most, now in flight; stored ones leave. */
  List<Bytes32> take(final int max) {
    final List<Bytes32> batch = new ArrayList<>();
    final Iterator<Bytes32> iterator = queued.iterator();
    while (iterator.hasNext() && batch.size() < max) {
      final Bytes32 codeHash = iterator.next();
      iterator.remove();
      if (!isStored(codeHash)) {
        batch.add(codeHash);
      }
    }
    inFlight.addAll(batch);
    return batch;
  }

  /** Ends the request of a batch: the codes it did not store wait in the queue again. */
  void answered(final List<Bytes32> batch, final Set<Bytes32> stored) {
    for (final Bytes32 codeHash : batch) {
      // a code answered already is no longer in flight
      final boolean wasInFlight = inFlight.remove(codeHash);
      if (wasInFlight && !stored.contains(codeHash)) {
        queued.add(codeHash);
      }
    }
  }

  // a failed lookup only costs a download
  private boolean isStored(final Bytes32 codeHash) {
    try {
      return storage.hasCode(codeHash);
    } catch (final RuntimeException e) {
      LOG.debug("Failed to look up bytecode {}", codeHash, e);
      return false;
    }
  }
}
