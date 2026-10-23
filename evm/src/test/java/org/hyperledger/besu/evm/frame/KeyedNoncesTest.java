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
package org.hyperledger.besu.evm.frame;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.fluent.SimpleWorld;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class KeyedNoncesTest {

  private static final UInt256 ONE = UInt256.ONE;
  private static final UInt256 TWO = UInt256.valueOf(2);

  @Test
  void acceptsTheLegacySetAndStrictlyIncreasingKeys() {
    assertThat(KeyedNonces.validate(KeyedNonces.LEGACY_NONCE_KEYS)).isEmpty();
    assertThat(KeyedNonces.validate(List.of(ONE, TWO))).isEmpty();
    assertThat(KeyedNonces.isLegacy(KeyedNonces.LEGACY_NONCE_KEYS)).isTrue();
    assertThat(KeyedNonces.isLegacy(List.of(ONE))).isFalse();
  }

  @Test
  void rejectsMalformedKeySets() {
    assertThat(KeyedNonces.validate(Collections.emptyList())).contains("invalid nonce key count");
    final List<UInt256> seventeen =
        IntStream.rangeClosed(1, 17).mapToObj(UInt256::valueOf).toList();
    assertThat(KeyedNonces.validate(seventeen)).contains("invalid nonce key count");
    assertThat(KeyedNonces.validate(List.of(TWO, ONE)))
        .contains("nonce keys not strictly increasing");
    assertThat(KeyedNonces.validate(List.of(ONE, ONE)))
        .contains("nonce keys not strictly increasing");
    assertThat(KeyedNonces.validate(List.of(UInt256.ZERO, ONE)))
        .contains("zero nonce key alongside other keys");
  }

  @Test
  void slotsDependOnSenderAndKey() {
    final Address a = Address.fromHexString("0x01");
    final Address b = Address.fromHexString("0x02");
    assertThat(KeyedNonces.nonceSlot(a, ONE)).isEqualTo(KeyedNonces.nonceSlot(a, ONE));
    assertThat(KeyedNonces.nonceSlot(a, ONE)).isNotEqualTo(KeyedNonces.nonceSlot(a, TWO));
    assertThat(KeyedNonces.nonceSlot(a, ONE)).isNotEqualTo(KeyedNonces.nonceSlot(b, ONE));
  }

  @Test
  void sequencesAreUnsigned() {
    assertThat(KeyedNonces.unsigned(-1L)).isEqualTo(UInt256.fromHexString("0xffffffffffffffff"));
  }

  private static final Address SENDER = Address.fromHexString("0x01");

  @Test
  void slotIsTheKeccakOfThePaddedSenderAndTheKey() {
    assertThat(KeyedNonces.nonceSlot(SENDER, TWO))
        .isEqualTo(
            UInt256.fromBytes(
                Hash.hash(Bytes.concatenate(Bytes32.leftPad(SENDER.getBytes()), TWO)).getBytes()));
  }

  @Test
  void zeroKeyAliasesTheAccountNonce() {
    final SimpleWorld world = new SimpleWorld();
    final RecordingAccessList accessList = new RecordingAccessList();
    assertThat(KeyedNonces.currentNonceSeq(world, SENDER, UInt256.ZERO, Optional.of(accessList)))
        .isEqualTo(UInt256.ZERO);
    world.createAccount(SENDER, 7L, Wei.ZERO);
    assertThat(KeyedNonces.currentNonceSeq(world, SENDER, UInt256.ZERO, Optional.of(accessList)))
        .isEqualTo(UInt256.valueOf(7));
    // The account nonce is not a nonce manager slot.
    assertThat(accessList.slots).isEmpty();
  }

  @Test
  void keyedSequencesAreReadFromTheNonceManager() {
    final SimpleWorld world = new SimpleWorld();
    final RecordingAccessList accessList = new RecordingAccessList();
    // No nonce manager yet: every keyed sequence is zero.
    assertThat(KeyedNonces.currentNonceSeq(world, SENDER, ONE, Optional.of(accessList)))
        .isEqualTo(UInt256.ZERO);

    world
        .createAccount(KeyedNonces.NONCE_MANAGER, 1L, Wei.ZERO)
        .setStorageValue(KeyedNonces.nonceSlot(SENDER, ONE), UInt256.valueOf(9));
    assertThat(KeyedNonces.currentNonceSeq(world, SENDER, ONE, Optional.of(accessList)))
        .isEqualTo(UInt256.valueOf(9));
    assertThat(KeyedNonces.currentNonceSeq(world, SENDER, TWO, Optional.empty()))
        .isEqualTo(UInt256.ZERO);
    assertThat(accessList.slots)
        .containsExactly(KeyedNonces.nonceSlot(SENDER, ONE), KeyedNonces.nonceSlot(SENDER, ONE));
  }

  @Test
  void countFirstUsesCountsUnusedKeys() {
    final SimpleWorld world = new SimpleWorld();
    assertThat(KeyedNonces.countFirstUses(world, SENDER, List.of(ONE, TWO), Optional.empty()))
        .isEqualTo(2);
    world
        .createAccount(KeyedNonces.NONCE_MANAGER, 1L, Wei.ZERO)
        .setStorageValue(KeyedNonces.nonceSlot(SENDER, TWO), UInt256.ONE);
    assertThat(KeyedNonces.countFirstUses(world, SENDER, List.of(ONE, TWO), Optional.empty()))
        .isEqualTo(1);
  }

  @Test
  void consumeKeyedAdvancesEverySelectedKey() {
    final SimpleWorld world = new SimpleWorld();
    world.createAccount(SENDER, 3L, Wei.ZERO);
    final RecordingAccessList accessList = new RecordingAccessList();

    KeyedNonces.consumeKeyed(world, SENDER, List.of(ONE, TWO), 4L, Optional.of(accessList));

    assertThat(KeyedNonces.currentNonceSeq(world, SENDER, ONE, Optional.empty()))
        .isEqualTo(UInt256.valueOf(5));
    assertThat(KeyedNonces.currentNonceSeq(world, SENDER, TWO, Optional.empty()))
        .isEqualTo(UInt256.valueOf(5));
    assertThat(KeyedNonces.countFirstUses(world, SENDER, List.of(ONE, TWO), Optional.empty()))
        .isZero();
    // The account nonce belongs to the legacy key set and is left alone.
    assertThat(world.get(SENDER).getNonce()).isEqualTo(3L);
    assertThat(accessList.slots)
        .containsExactly(KeyedNonces.nonceSlot(SENDER, ONE), KeyedNonces.nonceSlot(SENDER, TWO));
  }

  @Test
  void consumeKeyedStoresTheNextSequenceUnsigned() {
    final SimpleWorld world = new SimpleWorld();
    KeyedNonces.consumeKeyed(
        world, SENDER, List.of(ONE), KeyedNonces.MAX_NONCE_SEQ - 1, Optional.empty());
    assertThat(KeyedNonces.currentNonceSeq(world, SENDER, ONE, Optional.empty()))
        .isEqualTo(KeyedNonces.unsigned(KeyedNonces.MAX_NONCE_SEQ));
  }

  @Test
  void nonceKeysHashCommitsToTheCountAndEveryKey() {
    assertThat(KeyedNonces.nonceKeysHash(KeyedNonces.LEGACY_NONCE_KEYS))
        .isEqualTo(
            Bytes32.wrap(Hash.hash(Bytes.concatenate(UInt256.ONE, UInt256.ZERO)).getBytes()));
    assertThat(KeyedNonces.nonceKeysHash(List.of(ONE, TWO)))
        .isEqualTo(
            Bytes32.wrap(Hash.hash(Bytes.concatenate(UInt256.valueOf(2), ONE, TWO)).getBytes()));
    assertThat(KeyedNonces.nonceKeysHash(List.of(ONE)))
        .isNotEqualTo(KeyedNonces.nonceKeysHash(List.of(TWO)));
  }

  private static final class RecordingAccessList implements Eip7928AccessList {
    private final List<UInt256> slots = new ArrayList<>();

    @Override
    public void addTouchedAccount(final Address address) {}

    @Override
    public void addSlotAccessForAccount(final Address address, final UInt256 slotKey) {
      assertThat(address).isEqualTo(KeyedNonces.NONCE_MANAGER);
      slots.add(slotKey);
    }

    @Override
    public void clear() {
      slots.clear();
    }
  }
}
