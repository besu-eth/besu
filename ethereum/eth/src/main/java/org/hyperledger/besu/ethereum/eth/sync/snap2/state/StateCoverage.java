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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.tuweni.bytes.Bytes32;

/**
 * What the flat state holds at the current pivot: the persisted account ranges and, for accounts in
 * them whose storage is not complete yet, the persisted slot ranges.
 */
final class StateCoverage {

  private final KeyRangeSet accounts = new KeyRangeSet();
  private final ConcurrentHashMap<Bytes32, StorageTask> storage = new ConcurrentHashMap<>();

  /** Returns the persisted account ranges. */
  public KeyRangeSet accounts() {
    return accounts;
  }

  /** Returns the pending storage of an account, or null if its storage is fully known. */
  public StorageTask storageTask(final Bytes32 accountHash) {
    return storage.get(accountHash);
  }

  /** Returns whether a task is the pending storage of its account. */
  public boolean isPending(final StorageTask task) {
    return storage.get(task.accountHash()) == task;
  }

  public void addStorageTask(final StorageTask task) {
    storage.put(task.accountHash(), task);
  }

  /** Removes the pending storage of an account, whichever task it is. */
  public void removeStorageTask(final Bytes32 accountHash) {
    storage.remove(accountHash);
  }

  /** Removes a task while it is the pending storage of its account, never a newer task. */
  public void removeStorageTask(final StorageTask task) {
    storage.remove(task.accountHash(), task);
  }

  public int pendingStorageCount() {
    return storage.size();
  }
}
