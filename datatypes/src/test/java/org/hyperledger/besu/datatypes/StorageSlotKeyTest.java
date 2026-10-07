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

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class StorageSlotKeyTest {

  private static final UInt256 SLOT = UInt256.valueOf(7);

  @Test
  void keysOfTheSameSlotAreEqual() {
    final StorageSlotKey key = new StorageSlotKey(SLOT);
    final StorageSlotKey sameSlot = new StorageSlotKey(UInt256.valueOf(7));

    assertThat(key).isEqualTo(sameSlot).hasSameHashCodeAs(sameSlot);
    assertThat(key).isNotEqualTo(new StorageSlotKey(UInt256.valueOf(8)));
  }

  @Test
  void equalityDependsOnTheHashOnly() {
    final StorageSlotKey key = new StorageSlotKey(SLOT);
    // same hash held by another Bytes implementation, without the slot
    final MutableBytes32 hashBytes = MutableBytes32.create();
    Bytes.wrap(key.getSlotHash().getBytes().toArray()).copyTo(hashBytes);
    final StorageSlotKey hashOnly =
        new StorageSlotKey(Hash.wrap(hashBytes.copy()), Optional.empty());

    assertThat(key).isEqualTo(hashOnly).hasSameHashCodeAs(hashOnly);
  }
}
