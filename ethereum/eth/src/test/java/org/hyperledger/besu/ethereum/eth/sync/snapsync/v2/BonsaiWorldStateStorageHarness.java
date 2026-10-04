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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import static org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator.applyForStrategy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Bonsai flat-DB backed harness. Storage root passed to seedAccount is ignored; Bonsai derives it.
 */
final class BonsaiWorldStateStorageHarness implements WorldStateStorageHarness {

  private final WorldStateStorageCoordinator coordinator =
      new WorldStateStorageCoordinator(
          new BonsaiWorldStateKeyValueStorage(
              new InMemoryKeyValueStorageProvider(),
              new NoOpMetricsSystem(),
              DataStorageConfiguration.DEFAULT_BONSAI_CONFIG));

  @Override
  public WorldStateStorageCoordinator coordinator() {
    return coordinator;
  }

  @Override
  public void seedAccount(
      final Address address,
      final long nonce,
      final Wei balance,
      final Hash storageRoot,
      final Hash codeHash) {
    final PmtStateTrieAccountValue account =
        new PmtStateTrieAccountValue(nonce, balance, storageRoot, codeHash);
    final Bytes encoded = RLP.encode(account::writeTo);
    final WorldStateKeyValueStorage.Updater updater = coordinator.updater();
    applyForStrategy(
        updater,
        bonsai -> bonsai.putAccountInfoState(address.addressHash(), encoded),
        forest -> {});
    updater.commit();
  }

  @Override
  public void seedStorageSlot(final Address address, final UInt256 slotKey, final UInt256 value) {
    final WorldStateKeyValueStorage.Updater updater = coordinator.updater();
    applyForStrategy(
        updater,
        bonsai ->
            bonsai.putStorageValueBySlotHash(
                address.addressHash(), Hash.hash(slotKey.toBytes()), value.toBytes()),
        forest -> {});
    updater.commit();
  }

  @Override
  public void seedCode(final Address address, final Bytes code) {
    final Hash codeHash = Hash.hash(code);
    final WorldStateKeyValueStorage.Updater updater = coordinator.updater();
    applyForStrategy(
        updater, bonsai -> bonsai.putCode(address.addressHash(), codeHash, code), forest -> {});
    updater.commit();
    // Rewrite the account so its codeHash points at the seeded code.
    readAccount(address)
        .ifPresent(
            a -> seedAccount(address, a.getNonce(), a.getBalance(), a.getStorageRoot(), codeHash));
  }

  @Override
  public Optional<PmtStateTrieAccountValue> readAccount(final Address address) {
    return coordinator
        .applyForStrategy(
            bonsai -> bonsai.getAccount(address.addressHash()), forest -> Optional.<Bytes>empty())
        .map(b -> PmtStateTrieAccountValue.readFrom(RLP.input(b)));
  }

  @Override
  public Optional<UInt256> readStorageSlot(final Address address, final UInt256 slotKey) {
    return coordinator
        .applyForStrategy(
            bonsai ->
                bonsai.getStorageValueByStorageSlotKey(
                    address.addressHash(), new StorageSlotKey(slotKey)),
            forest -> Optional.<Bytes>empty())
        .map(UInt256::fromBytes);
  }

  @Override
  public Optional<Bytes> readCode(final Address address) {
    return readAccount(address)
        .flatMap(
            a ->
                coordinator.applyForStrategy(
                    bonsai -> bonsai.getCode(a.getCodeHash(), address.addressHash()),
                    forest -> Optional.<Bytes>empty()));
  }

  @Override
  public String label() {
    return "BONSAI";
  }

  @Override
  public String toString() {
    return label();
  }
}
