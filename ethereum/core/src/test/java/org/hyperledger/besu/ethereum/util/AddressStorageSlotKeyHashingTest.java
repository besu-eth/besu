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
package org.hyperledger.besu.ethereum.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.internal.AddressStorageSlotKey;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class AddressStorageSlotKeyHashingTest {

  private static final int KEYS = 300_000;

  private static Stream<Arguments> addressArgs() {
    final Random rand = new Random(234234L);
    return Stream.concat(
        Stream.of(Arguments.of(Address.ZERO)),
        IntStream.range(0, 10).mapToObj(__ -> Arguments.of(Address.wrap(Bytes.random(20, rand)))));
  }

  @ParameterizedTest
  @MethodSource("addressArgs")
  void hashCodeKeysCollide(final Address address) throws Exception {
    final Set<Integer> hashCodes = new HashSet<>();
    final Set<Bytes32> slots = new HashSet<>();
    final Set<AddressStorageSlotKey> keys = new HashSet<>();

    for (int i = 0; i < KEYS; i++) {
      final Bytes32 slot = AddressStorageSlotKeyHashing.collidingHash(address, i);
      slots.add(slot);
      final AddressStorageSlotKey key = new AddressStorageSlotKey(address, slot);
      hashCodes.add(key.hashCode());
      keys.add(key);
    }

    assertThat(slots).hasSize(KEYS);
    assertThat(keys).hasSize(KEYS);
    assertThat(hashCodes).hasSize(1);
  }

  @ParameterizedTest
  @MethodSource("addressArgs")
  void hashCodeKeysAreDistinct(final Address address) throws Exception {
    final Set<Integer> hashCodes = new HashSet<>();
    final Set<Bytes32> slots = new HashSet<>();

    for (int i = 0; i < KEYS; i++) {
      final Bytes32 slot = AddressStorageSlotKeyHashing.distinctHash(address, i);
      slots.add(slot);
      hashCodes.add(new AddressStorageSlotKey(address, slot).hashCode());
    }

    assertThat(slots).hasSize(KEYS);
    assertThat(hashCodes).hasSize(KEYS);
  }
}
