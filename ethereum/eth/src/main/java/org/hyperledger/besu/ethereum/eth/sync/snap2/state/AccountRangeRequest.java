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

import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SnapMessages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.RangeProof;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Requests the accounts of a key range at a state root and verifies the range proof. */
final class AccountRangeRequest implements SyncRequest<AccountRangeRequest.Result> {

  /**
   * A verified account range.
   *
   * @param hashes the account hashes
   * @param accounts the full account encodings
   * @param hasMore whether accounts exist after the last one
   */
  record Result(List<Bytes32> hashes, List<Bytes> accounts, boolean hasMore) {
    /** Returns whether the first account served is this one, as when asked for it alone. */
    boolean startsWith(final Bytes32 hash) {
      return !hashes.isEmpty() && hashes.getFirst().equals(hash);
    }
  }

  final Pivot pivot;
  final Bytes32 origin;
  final Bytes32 limit;
  private final long bytes;
  private final String name;

  AccountRangeRequest(
      final Pivot pivot,
      final Bytes32 origin,
      final Bytes32 limit,
      final long bytes,
      final String name) {
    this.pivot = pivot;
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
    return SnapMessages.getAccountRange(pivot.root(), origin, limit, bytes);
  }

  @Override
  public Result decode(final MessageData response, final EthPeer peer) {
    final SnapMessages.AccountRange range = SnapMessages.accountRange(response);
    if (range.hashes().isEmpty() && range.proof().isEmpty()) {
      throw new EmptyResponseException("no account range for root " + pivot.root());
    }
    final RangeProof.Result verified =
        RangeProof.verify(pivot.root(), origin, range.hashes(), range.accounts(), range.proof());
    if (!verified.valid()) {
      throw new InvalidResponseException("invalid account range proof", true);
    }
    return new Result(range.hashes(), range.accounts(), verified.hasMore());
  }
}
