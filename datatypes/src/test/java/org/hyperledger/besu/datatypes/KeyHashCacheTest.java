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
package org.hyperledger.besu.datatypes;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class KeyHashCacheTest {

  private static final UInt256 SLOT = UInt256.valueOf(7);

  @Test
  void hashesEachSlotOnce() {
    final KeyHashCache cache = new KeyHashCache();

    final Hash first = cache.slotHash(SLOT);

    assertThat(first).isEqualTo(Hash.hash(SLOT));
    // same instance: computed once
    assertThat(cache.slotHash(SLOT)).isSameAs(first);
    assertThat(cache.slotKey(SLOT)).isEqualTo(new StorageSlotKey(SLOT));
    assertThat(cache.slotKey(SLOT).getSlotHash()).isSameAs(first);
  }

  @Test
  void addKeepsAKnownHash() {
    final KeyHashCache cache = new KeyHashCache();
    final StorageSlotKey known = new StorageSlotKey(SLOT);

    cache.add(known);

    assertThat(cache.slotHash(SLOT)).isSameAs(known.getSlotHash());
  }

  @Test
  void hashesEachAddressOnceWhateverTheInstance() {
    final KeyHashCache cache = new KeyHashCache();
    final Address first = Address.fromHexString("0x5107");
    final Address other = Address.fromHexString("0x5107");

    final Hash hash = cache.addressHash(first);

    assertThat(hash).isEqualTo(Hash.hash(first.getBytes()));
    // the other instance gets the hash of the first, and keeps it
    assertThat(cache.addressHash(other)).isSameAs(hash);
    assertThat(other.addressHash()).isSameAs(hash);
  }

  @Test
  void clearForgetsTheHashes() {
    final KeyHashCache cache = new KeyHashCache();
    final Hash before = cache.slotHash(SLOT);

    cache.clear();

    assertThat(cache.slotHash(SLOT)).isEqualTo(before).isNotSameAs(before);
  }
}
