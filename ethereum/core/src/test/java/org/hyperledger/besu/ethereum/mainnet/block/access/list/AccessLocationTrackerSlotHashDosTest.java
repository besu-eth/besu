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
package org.hyperledger.besu.ethereum.mainnet.block.access.list;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker.AccountAccessList;

import java.util.NavigableSet;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/**
 * Regression test for the per-account EIP-7928 slot-tracking HashDoS.
 *
 * <p>{@code UInt256.computeHashcode} is a base-31 byte polynomial, identical in form to Tuweni's
 * {@code Bytes} hash and grindable in exactly the same way. Unlike {@link Address}, {@link UInt256}
 * does not declare {@code Comparable<UInt256>} among its interfaces - it declares only {@code
 * UInt256Value<UInt256>} - and {@code HashMap.comparableClassFor()} only treeifies a bin when the
 * key declares {@code Comparable} of its own exact type. A hash-based set of {@code UInt256}
 * therefore walks a colliding bin linearly.
 *
 * <p>Storage slot keys are chosen directly by the executing contract, so this was attacker
 * controlled. Measured against the real sink before the fix: 2,000 colliding slots 25ms, 4,000
 * 104ms, 8,000 490ms - 4x per doubling, i.e. quadratic. After the fix, 8,000 slots cost ~5ms.
 *
 * <p>This test asserts the structural invariant rather than elapsed time, so it is deterministic
 * and cannot flake under CI load.
 */
class AccessLocationTrackerSlotHashDosTest {

  private static final int SLOT_COUNT = 4_000;

  /**
   * Two bytes (a, b) contribute {@code 31*a + b} to the base-31 polynomial at a's position. Each of
   * these three pairs contributes exactly zero, so tiling any combination of them leaves the
   * hashCode unchanged while the underlying value differs.
   */
  private static void writeZeroSumPair(final byte[] bytes, final int offset, final int digit) {
    switch (digit) {
      case 0 -> {
        bytes[offset] = 0;
        bytes[offset + 1] = 0;
      }
      case 1 -> {
        bytes[offset] = 1;
        bytes[offset + 1] = (byte) -31;
      }
      default -> {
        bytes[offset] = (byte) -1;
        bytes[offset + 1] = 31;
      }
    }
  }

  private static UInt256 collidingSlot(final int index) {
    final byte[] bytes = new byte[32];
    long remaining = index;
    for (int pair = 0; pair < 16; pair++) {
      writeZeroSumPair(bytes, pair * 2, (int) (remaining % 3));
      remaining /= 3;
    }
    return UInt256.fromBytes(Bytes32.wrap(bytes));
  }

  @Test
  void generatedSlotKeysActuallyCollide() {
    final int firstHash = collidingSlot(0).hashCode();
    for (int i = 1; i < 500; i++) {
      assertThat(collidingSlot(i).hashCode())
          .as("slot %s must share the family's hashCode", i)
          .isEqualTo(firstHash);
      assertThat(collidingSlot(i))
          .as("slot %s must still be a distinct value", i)
          .isNotEqualTo(collidingSlot(0));
    }
  }

  @Test
  void perAccountSlotTrackingIsSortedNotHashed() {
    final AccountAccessList account = new AccountAccessList(Address.ZERO);
    for (int i = 0; i < SLOT_COUNT; i++) {
      account.addSlotAccess(collidingSlot(i));
    }

    // The invariant: slot tracking must not be backed by a hash-based set, because UInt256 keys
    // cannot treeify and a colliding bin would be walked linearly on every insert.
    assertThat(account.getSlots())
        .as(
            "per-account slot tracking must use a sorted set - UInt256 does not declare "
                + "Comparable<UInt256>, so a hash-based set cannot treeify a colliding bin and "
                + "chosen-collision slot keys make insertion quadratic")
        .isInstanceOf(NavigableSet.class);

    // ...and it must still behave as a correct set of exactly the slots that were touched.
    assertThat(account.getSlots()).hasSize(SLOT_COUNT);
    for (int i = 0; i < SLOT_COUNT; i++) {
      assertThat(account.getSlots()).contains(collidingSlot(i));
    }
  }
}
