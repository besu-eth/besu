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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.internal.AddressStorageSlotKey;

import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class AddressStorageSlotKeyHashingTest {
  private static Stream<Arguments> addressArgs() {
    final Random rand = new Random(234234L);
    return Stream.concat(
        Stream.of(Arguments.of(Address.ZERO)),
        IntStream.range(0, 10).mapToObj(__ -> Arguments.of(Address.wrap(Bytes.random(20, rand)))));
  }

  @ParameterizedTest
  @MethodSource("addressArgs")
  void hashCodeKeysCollide(final Address address) throws Exception {
    for (int i = 0; i < 1_000; i++) {
      for (int j = 0; j < i; j++) {
        final AddressStorageSlotKey key_i =
            new AddressStorageSlotKey(
                address, AddressStorageSlotKeyHashing.collidingHash(address, i));
        final AddressStorageSlotKey key_j =
            new AddressStorageSlotKey(
                address, AddressStorageSlotKeyHashing.collidingHash(address, j));

        assertThat(key_i.hashCode()).isEqualTo(key_j.hashCode());
        assertThat(key_i).isNotEqualTo(key_j);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("addressArgs")
  void hashCodeKeysAreDistinct(final Address address) throws Exception {
    for (int i = 0; i < 1_000; i++) {
      for (int j = 0; j < i; j++) {
        final AddressStorageSlotKey key_i =
            new AddressStorageSlotKey(
                address, AddressStorageSlotKeyHashing.distinctHash(address, i));
        final AddressStorageSlotKey key_j =
            new AddressStorageSlotKey(
                address, AddressStorageSlotKeyHashing.distinctHash(address, j));

        assertThat(key_i.hashCode()).isNotEqualTo(key_j.hashCode());
        assertThat(key_i).isNotEqualTo(key_j);
      }
    }
  }
}
