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

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SnapMessages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.RangeProof;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Requests the storage of several accounts, or a slot range of one account, and verifies every
 * returned range against the storage root of its account.
 */
final class StorageRangesRequest implements SyncRequest<StorageRangesRequest.Result> {

  /**
   * Verified slots of one account.
   *
   * @param root the storage root they were verified against
   * @param hashes the slot hashes
   * @param values the RLP encoded slot values
   * @param proof the proof of their edges, empty for a whole storage
   * @param complete whether no slot exists after the last one
   */
  record Slots(
      Bytes32 root,
      List<Bytes32> hashes,
      List<Bytes> values,
      List<Bytes> proof,
      boolean complete) {}

  /**
   * Verified slots of the served accounts, in request order.
   *
   * @param accounts the slots of each served account
   */
  record Result(List<Slots> accounts) {}

  final Pivot pivot;
  final List<StorageTask> tasks;
  private final List<Bytes32> accountHashes;
  private final List<Bytes32> storageRoots;
  final Bytes32 origin;
  final Bytes32 limit;
  private final long bytes;
  private final String name;

  StorageRangesRequest(
      final Pivot pivot,
      final List<StorageTask> tasks,
      final Bytes32 origin,
      final Bytes32 limit,
      final long bytes,
      final String name) {
    // servers apply an origin differently to several storages, so only one may have it
    checkArgument(tasks.size() == 1 || origin.isZero(), "an origin for several storages");
    this.pivot = pivot;
    this.tasks = List.copyOf(tasks);
    this.accountHashes = tasks.stream().map(StorageTask::accountHash).toList();
    this.storageRoots = tasks.stream().map(StorageTask::storageRoot).toList();
    this.origin = origin;
    this.limit = limit;
    this.bytes = bytes;
    this.name = name;
  }

  @Override
  public Protocol protocol() {
    return Protocol.SNAP;
  }

  @Override
  public Priority priority() {
    return Priority.STATE;
  }

  @Override
  public long minimumPeerHeight() {
    return pivot.header().getNumber();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public MessageData message(final EthPeer peer) {
    return SnapMessages.getStorageRanges(pivot.root(), accountHashes, origin, limit, bytes);
  }

  @Override
  public Result decode(final MessageData response, final EthPeer peer) {
    final SnapMessages.StorageRanges ranges = SnapMessages.storageRanges(response);
    final int served = ranges.slotHashes().size();
    if (served > tasks.size()) {
      throw new InvalidResponseException("more storage ranges than requested", true);
    }
    if (served == 0) {
      if (ranges.proof().isEmpty()) {
        throw new EmptyResponseException("no storage for root " + pivot.root());
      }
      // a proof alone proves that the first account has no slot from the origin on
      return new Result(List.of(verify(0, List.of(), List.of(), ranges.proof())));
    }
    final List<Slots> accounts = new ArrayList<>(served);
    for (int i = 0; i < served; i++) {
      final boolean proven = i == served - 1 && !ranges.proof().isEmpty();
      accounts.add(
          verify(
              i,
              ranges.slotHashes().get(i),
              ranges.slotValues().get(i),
              proven ? ranges.proof() : List.of()));
    }
    return new Result(accounts);
  }

  private Slots verify(
      final int index,
      final List<Bytes32> hashes,
      final List<Bytes> values,
      final List<Bytes> proof) {
    final Bytes32 root = storageRoots.get(index);
    final RangeProof.Result verified = RangeProof.verify(root, origin, hashes, values, proof);
    if (!verified.valid()) {
      throw new InvalidResponseException("invalid storage range", true);
    }
    return new Slots(root, hashes, values, proof, !verified.hasMore());
  }
}
