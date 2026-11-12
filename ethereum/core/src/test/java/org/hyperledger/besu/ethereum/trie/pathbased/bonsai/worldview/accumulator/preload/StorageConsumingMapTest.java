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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class StorageConsumingMapTest {

  private static final Address ACCOUNT =
      Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final StorageSlotKey SLOT = new StorageSlotKey(UInt256.ONE);

  private final List<StorageSlotKey> preloaded = new ArrayList<>();
  private final StorageConsumingMap<StorageSlotKey, BonsaiValue<UInt256>> slots =
      new StorageConsumingMap<>(
          ACCOUNT, new ConcurrentHashMap<>(), (address, slotKey) -> preloaded.add(slotKey));

  @Test
  void preloadsASlotOnceWhenItBecomesChanged() {
    slots.put(SLOT, new BonsaiValue<>(UInt256.ONE, UInt256.ONE));
    assertThat(preloaded).isEmpty();

    slots.put(SLOT, new BonsaiValue<>(UInt256.ONE, UInt256.valueOf(2)));
    slots.put(SLOT, new BonsaiValue<>(UInt256.ONE, UInt256.valueOf(3)));

    assertThat(preloaded).containsExactly(SLOT);
  }

  @Test
  void update_preloadsASlotOnceWhenItBecomesChanged() {
    final BonsaiValue<UInt256> slot = readSlot(UInt256.ONE);
    slots.putLoaded(SLOT, slot);

    slots.update(SLOT, slot, UInt256.valueOf(2));
    slots.update(SLOT, slot, UInt256.valueOf(3));

    assertThat(preloaded).containsExactly(SLOT);
    assertThat(slot.getUpdated()).isEqualTo(UInt256.valueOf(3));
  }

  @Test
  void update_toThePriorValue_preloadsNothing() {
    final BonsaiValue<UInt256> slot = readSlot(UInt256.ONE);
    slots.putLoaded(SLOT, slot);

    slots.update(SLOT, slot, UInt256.ONE);

    assertThat(preloaded).isEmpty();
  }

  @Test
  void putLoaded_neitherPreloadsNorLoadsThePrior() {
    final AtomicInteger loads = new AtomicInteger();
    final Supplier<UInt256> loader =
        Suppliers.memoize(
            () -> {
              loads.incrementAndGet();
              return UInt256.ONE;
            });
    final BonsaiValue<UInt256> value = BonsaiValue.withLazy(loader, loader);
    // a block access list overlays the value before it is read
    value.setUpdated(UInt256.valueOf(2));

    slots.putLoaded(SLOT, value);

    assertThat(preloaded).isEmpty();
    assertThat(loads).hasValue(0);
    assertThat(slots.get(SLOT)).isSameAs(value);
  }

  /** A slot as a read loads it, with one supplier for prior and updated. */
  private static BonsaiValue<UInt256> readSlot(final UInt256 value) {
    final Supplier<UInt256> loader = () -> value;
    return BonsaiValue.withLazy(loader, loader);
  }
}
