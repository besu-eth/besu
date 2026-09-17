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
package org.hyperledger.besu.evm.frame;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.toy.ToyBlockValues;
import org.hyperledger.besu.evm.toy.ToyWorld;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/**
 * Regression test asserting that an attacker who grinds many {@link Address}/{@link Bytes32} keys
 * sharing the same (base-31, non-treeifying) {@code hashCode()} cannot force O(n) bucket walks per
 * {@code TSTORE}/warm-up insert. {@link MessageFrame}'s warm-address, warm-storage and
 * transient-storage collections are keyed on exactly such colliding values below, and the whole
 * batch of inserts is required to complete well inside a budget that a quadratic blow-up would blow
 * through by orders of magnitude.
 */
class WarmStorageHashDosTest {

  private static final int SLOT_COUNT = 50_000;
  private static final int ADDRESS_COUNT = 50_000;

  /**
   * Two bytes (a, b) contribute {@code 31*a + b} to Tuweni's base-31 polynomial {@code hashCode()}
   * at the position of {@code a}. Each of these three pairs contributes exactly zero, so tiling any
   * combination of them across a key leaves its hashCode unchanged (all keys collide into a single
   * hash bucket) while the underlying bytes - and thus the keys themselves - remain distinct.
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

  private static Bytes32 collidingSlot(final long index) {
    final byte[] bytes = new byte[32];
    long remaining = index;
    for (int pair = 0; pair < 16; pair++) {
      writeZeroSumPair(bytes, pair * 2, (int) (remaining % 3));
      remaining /= 3;
    }
    return Bytes32.wrap(bytes);
  }

  private static Address collidingAddress(final long index) {
    final byte[] bytes = new byte[Address.SIZE];
    long remaining = index;
    for (int pair = 0; pair < Address.SIZE / 2; pair++) {
      writeZeroSumPair(bytes, pair * 2, (int) (remaining % 3));
      remaining /= 3;
    }
    return Address.wrap(Bytes.wrap(bytes));
  }

  private static MessageFrame newFrame() {
    return MessageFrame.builder()
        .worldUpdater(new ToyWorld())
        .originator(Address.ZERO)
        .gasPrice(Wei.ONE)
        .blobGasPrice(Wei.ONE)
        .blockValues(new ToyBlockValues())
        .miningBeneficiary(Address.ZERO)
        .blockHashLookup((__, ___) -> Hash.ZERO)
        .type(MessageFrame.Type.MESSAGE_CALL)
        .initialGas(1)
        .address(Address.ZERO)
        .contract(Address.ZERO)
        .inputData(Bytes32.ZERO)
        .sender(Address.ZERO)
        .value(Wei.ZERO)
        .apparentValue(Wei.ZERO)
        .code(Code.EMPTY_CODE)
        .completer(messageFrame -> {})
        .build();
  }

  @Test
  void generatedSlotsActuallyCollide() {
    final int hash0 = collidingSlot(0).hashCode();
    for (long i = 1; i < 1_000; i++) {
      assertThat(collidingSlot(i).hashCode()).isEqualTo(hash0);
      assertThat(collidingSlot(i)).isNotEqualTo(collidingSlot(0));
    }
  }

  @Test
  void generatedAddressesActuallyCollide() {
    final int hash0 = collidingAddress(0).hashCode();
    for (long i = 1; i < 1_000; i++) {
      assertThat(collidingAddress(i).hashCode()).isEqualTo(hash0);
      assertThat(collidingAddress(i)).isNotEqualTo(collidingAddress(0));
    }
  }

  @Test
  void transientStorageResistsHashCollisionFlood() {
    final MessageFrame frame = newFrame();
    final List<Bytes32> slots = new ArrayList<>(SLOT_COUNT);
    for (long i = 0; i < SLOT_COUNT; i++) {
      slots.add(collidingSlot(i));
    }

    assertTimeoutPreemptively(
        ofSeconds(10),
        () -> {
          for (final Bytes32 slot : slots) {
            frame.setTransientStorageValue(Address.ZERO, slot, slot);
          }
        });

    for (final Bytes32 slot : slots) {
      assertThat(frame.getTransientStorageValue(Address.ZERO, slot)).isEqualTo(slot);
    }
  }

  @Test
  void warmedUpStorageResistsHashCollisionFlood() {
    final MessageFrame frame = newFrame();
    final List<Bytes32> slots = new ArrayList<>(SLOT_COUNT);
    for (long i = 0; i < SLOT_COUNT; i++) {
      slots.add(collidingSlot(i));
    }

    assertTimeoutPreemptively(
        ofSeconds(10),
        () -> {
          for (final Bytes32 slot : slots) {
            frame.warmUpStorage(Address.ZERO, slot);
          }
        });

    for (final Bytes32 slot : slots) {
      assertThat(frame.getWarmedUpStorage().contains(Address.ZERO, slot)).isTrue();
    }
  }

  @Test
  void warmedUpAddressesResistHashCollisionFlood() {
    final MessageFrame frame = newFrame();
    final List<Address> addresses = new ArrayList<>(ADDRESS_COUNT);
    for (long i = 0; i < ADDRESS_COUNT; i++) {
      addresses.add(collidingAddress(i));
    }

    assertTimeoutPreemptively(
        ofSeconds(10),
        () -> {
          for (final Address address : addresses) {
            frame.warmUpAddress(address);
          }
        });

    for (final Address address : addresses) {
      assertThat(frame.isAddressWarm(address)).isTrue();
    }
  }
}
