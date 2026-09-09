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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

abstract class SnapV2TestFixtures {

  protected static final Address ALICE =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  protected static final Address BOB =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  protected static final Address CAROL =
      Address.fromHexString("0x3333333333333333333333333333333333333333");
  protected static final Address DAVE =
      Address.fromHexString("0x4444444444444444444444444444444444444444");
  protected static final Address EVE =
      Address.fromHexString("0x5555555555555555555555555555555555555555");
  protected static final Address FRANK =
      Address.fromHexString("0x6666666666666666666666666666666666666666");
  protected static final Address GRACE =
      Address.fromHexString("0x7777777777777777777777777777777777777777");
  protected static final Address NEW_CONTRACT =
      Address.fromHexString("0x9999999999999999999999999999999999999999");
  protected static final Address PETE =
      Address.fromHexString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
  protected static final Address PAULA =
      Address.fromHexString("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
  protected static final Address GHOST =
      Address.fromHexString("0xcccccccccccccccccccccccccccccccccccccccc");
  protected static final Address UNKNOWN =
      Address.fromHexString("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

  protected static final Bytes32 MAX_KEY =
      Bytes32.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

  protected static BonsaiWorldStateKeyValueStorage newBonsaiStorage() {
    return new BonsaiWorldStateKeyValueStorage(
        new InMemoryKeyValueStorageProvider(),
        new NoOpMetricsSystem(),
        DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  }

  protected static Hash worldStateRoot(final WorldStateStorageCoordinator coordinator) {
    return coordinator.getTrieNodeUnsafe(Bytes.EMPTY).map(Hash::hash).orElse(Hash.EMPTY_TRIE_HASH);
  }

  // ---- Tracker factories ----

  protected static DownloadedAccountRangeTracker fullAccountRange() {
    final DownloadedAccountRangeTracker tracker = new DownloadedAccountRangeTracker();
    tracker.registerPending(Bytes32.ZERO, MAX_KEY, 0);
    return tracker;
  }

  /**
   * Single-account ranges for the given accounts.
   *
   * @param completed if true the range has no outstanding child requests (leaves persisted); if
   *     false it has one outstanding child (leaves persisted but storage still downloading).
   */
  protected static DownloadedAccountRangeTracker accountRangeTracker(
      final boolean completed, final Address... accounts) {
    final DownloadedAccountRangeTracker tracker = new DownloadedAccountRangeTracker();
    final int pendingChildren = completed ? 0 : 1;
    for (final Address account : accounts) {
      final Bytes32 accountHash = Bytes32.wrap(account.addressHash().getBytes());
      tracker.registerPending(accountHash, accountHash, pendingChildren);
    }
    return tracker;
  }

  /** Registers single-slot ranges for the given slots of an account as downloaded. */
  protected static DownloadedStorageRangeTracker downloadedSlots(
      final Address account, final UInt256... slotKeys) {
    final DownloadedStorageRangeTracker tracker = new DownloadedStorageRangeTracker();
    final Bytes32 accountHash = Bytes32.wrap(account.addressHash().getBytes());
    for (final UInt256 slotKey : slotKeys) {
      final Bytes32 slotHash = Bytes32.wrap(ReorgBlockchainBuilder.slotHash(slotKey).getBytes());
      tracker.registerSlotRange(accountHash, slotHash, slotHash);
    }
    return tracker;
  }

  // ---- State readers ----

  protected static PmtStateTrieAccountValue readAccount(
      final WorldStateStorageCoordinator coordinator, final Address address) {
    return PmtStateTrieAccountValue.readFrom(
        RLP.input(readAccountBytes(coordinator, address).orElseThrow()));
  }

  protected static boolean accountExists(
      final WorldStateStorageCoordinator coordinator, final Address address) {
    return readAccountBytes(coordinator, address).isPresent();
  }

  protected static Optional<Bytes> readAccountBytes(
      final WorldStateStorageCoordinator coordinator, final Address address) {
    return coordinator.applyForStrategy(
        bonsai -> bonsai.getAccount(address.addressHash()), forest -> Optional.<Bytes>empty());
  }

  protected static Optional<UInt256> readStorageSlot(
      final WorldStateStorageCoordinator coordinator,
      final Address address,
      final UInt256 slotKey) {
    return coordinator
        .applyForStrategy(
            bonsai ->
                bonsai.getStorageValueByStorageSlotKey(
                    address.addressHash(), new StorageSlotKey(slotKey)),
            forest -> Optional.<Bytes>empty())
        .map(UInt256::fromBytes);
  }

  protected static Optional<Bytes> readCode(
      final WorldStateStorageCoordinator coordinator, final Address address) {
    final PmtStateTrieAccountValue account = readAccount(coordinator, address);
    return coordinator.applyForStrategy(
        bonsai -> bonsai.getCode(account.getCodeHash(), address.addressHash()),
        forest -> Optional.<Bytes>empty());
  }

  // ---- Hash helpers ----

  protected static Bytes32 accountHash(final Address address) {
    return Bytes32.wrap(address.addressHash().getBytes());
  }

  protected static Bytes32 slotHash(final UInt256 slotKey) {
    return Bytes32.wrap(ReorgBlockchainBuilder.slotHash(slotKey).getBytes());
  }
}
