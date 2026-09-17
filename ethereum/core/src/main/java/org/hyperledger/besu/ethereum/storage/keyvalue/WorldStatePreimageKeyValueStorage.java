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
package org.hyperledger.besu.ethereum.storage.keyvalue;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.WorldStatePreimageStorage;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

public class WorldStatePreimageKeyValueStorage implements WorldStatePreimageStorage {
  private final KeyValueStorage keyValueStorage;

  public WorldStatePreimageKeyValueStorage(final KeyValueStorage keyValueStorage) {
    this.keyValueStorage = keyValueStorage;
  }

  @Override
  public Optional<UInt256> getStorageTrieKeyPreimage(final Bytes32 trieKey) {
    return keyValueStorage
        .get(trieKey.toArrayUnsafe())
        .filter(val -> val.length == Bytes32.SIZE)
        .map(Bytes32::wrap)
        .map(UInt256::fromBytes);
  }

  @Override
  public Optional<Address> getAccountTrieKeyPreimage(final Bytes32 trieKey) {
    return keyValueStorage
        .get(trieKey.toArrayUnsafe())
        .filter(val -> val.length == Address.SIZE)
        .map(val -> Address.wrap(Bytes.wrap(val)));
  }

  @Override
  public Updater updater() {
    return new Updater(keyValueStorage.startTransaction());
  }

  public static class Updater implements WorldStatePreimageStorage.Updater {
    private final KeyValueStorageTransaction transaction;

    public Updater(final KeyValueStorageTransaction transaction) {
      this.transaction = transaction;
    }

    @Override
    public WorldStatePreimageStorage.Updater putStorageTrieKeyPreimage(
        final Bytes32 trieKey, final UInt256 preimage) {
      transaction.put(trieKey.toArrayUnsafe(), preimage.toArrayUnsafe());
      return this;
    }

    @Override
    public WorldStatePreimageStorage.Updater putAccountTrieKeyPreimage(
        final Bytes32 trieKey, final Address preimage) {
      transaction.put(trieKey.toArrayUnsafe(), preimage.getBytes().toArrayUnsafe());
      return this;
    }

    @Override
    public void commit() {
      transaction.commit();
    }

    @Override
    public void rollback() {
      transaction.rollback();
    }
  }
}
