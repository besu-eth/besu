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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.evm.worldstate.WorldView;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * EIP-8250 keyed nonces for frame transactions.
 *
 * <p>A frame transaction selects a set of nonce keys sharing one sequence number. The legacy key
 * set {@code [0]} aliases the sender's account nonce; every other key selects an independent
 * sequence held in the storage of the nonce manager contract. Keyed nonce reads and writes are
 * protocol bookkeeping: they bypass EIP-2929 warming and {@code SSTORE} pricing, but are recorded
 * in the EIP-7928 block access list.
 */
public final class KeyedNonces {

  /** The nonce manager contract, deployed as an ordinary contract and present in genesis. */
  public static final Address NONCE_MANAGER =
      Address.fromHexString("0x8250968C12e01A19d6F667b9B2F3b3A4d0e51cB7");

  /** Maximum number of nonce keys a frame transaction may select. */
  public static final int MAX_NONCE_KEYS = 16;

  /** The exhausted sequence, {@code 2^64-1} as an unsigned long: a key here cannot advance. */
  public static final long MAX_NONCE_SEQ = -1L;

  /** The key set selecting the sender's account nonce. */
  public static final List<UInt256> LEGACY_NONCE_KEYS = List.of(UInt256.ZERO);

  private KeyedNonces() {}

  /**
   * Whether the key set is the legacy set, selecting the account nonce.
   *
   * @param nonceKeys the transaction's nonce keys
   * @return true for {@code [0]}
   */
  public static boolean isLegacy(final List<UInt256> nonceKeys) {
    return nonceKeys.size() == 1 && nonceKeys.get(0).isZero();
  }

  /**
   * Checks that a key set is well formed: one to {@link #MAX_NONCE_KEYS} keys, strictly increasing,
   * with the zero key only ever alone.
   *
   * @param nonceKeys the transaction's nonce keys
   * @return the reason the set is malformed, if it is
   */
  public static Optional<String> validate(final List<UInt256> nonceKeys) {
    if (nonceKeys.isEmpty() || nonceKeys.size() > MAX_NONCE_KEYS) {
      return Optional.of("invalid nonce key count");
    }
    for (int i = 1; i < nonceKeys.size(); i++) {
      if (nonceKeys.get(i).compareTo(nonceKeys.get(i - 1)) <= 0) {
        return Optional.of("nonce keys not strictly increasing");
      }
    }
    if (nonceKeys.get(0).isZero() && nonceKeys.size() > 1) {
      return Optional.of("zero nonce key alongside other keys");
    }
    return Optional.empty();
  }

  /**
   * The nonce manager slot holding {@code sender}'s sequence for {@code nonceKey}: the keccak of
   * the left-padded sender followed by the big-endian key.
   *
   * @param sender the transaction sender
   * @param nonceKey a non-zero nonce key
   * @return the storage slot
   */
  public static UInt256 nonceSlot(final Address sender, final UInt256 nonceKey) {
    return UInt256.fromBytes(
        Hash.hash(Bytes.concatenate(Bytes32.leftPad(sender.getBytes()), nonceKey)).getBytes());
  }

  /**
   * The current sequence of {@code sender}'s nonce domain {@code nonceKey}. The zero key reads the
   * account nonce; any other key reads the nonce manager, where an absent slot is sequence zero.
   *
   * @param world the state to read
   * @param sender the transaction sender
   * @param nonceKey the nonce key
   * @param accessList the EIP-7928 access list recording the read, if any
   * @return the current sequence; a keyed slot can hold any 256-bit value, so this is not narrowed
   */
  public static UInt256 currentNonceSeq(
      final WorldView world,
      final Address sender,
      final UInt256 nonceKey,
      final Optional<? extends Eip7928AccessList> accessList) {
    if (nonceKey.isZero()) {
      final Account account = world.get(sender);
      return account == null ? UInt256.ZERO : unsigned(account.getNonce());
    }
    final UInt256 slot = nonceSlot(sender, nonceKey);
    accessList.ifPresent(list -> list.addSlotAccessForAccount(NONCE_MANAGER, slot));
    final Account manager = world.get(NONCE_MANAGER);
    return manager == null ? UInt256.ZERO : manager.getStorageValue(slot);
  }

  /**
   * A 64-bit sequence held in a {@code long} as an unsigned value.
   *
   * @param value the sequence
   * @return the same value as a 256-bit word
   */
  public static UInt256 unsigned(final long value) {
    return UInt256.fromBytes(Bytes32.leftPad(Bytes.ofUnsignedLong(value)));
  }

  /**
   * The number of selected keyed slots still unused, each of which consuming the set creates.
   *
   * @param world the state to read
   * @param sender the transaction sender
   * @param nonceKeys the transaction's nonce keys (not the legacy set)
   * @param accessList the EIP-7928 access list recording the reads, if any
   * @return the count of first uses
   */
  public static int countFirstUses(
      final WorldView world,
      final Address sender,
      final List<UInt256> nonceKeys,
      final Optional<? extends Eip7928AccessList> accessList) {
    int count = 0;
    for (final UInt256 key : nonceKeys) {
      if (currentNonceSeq(world, sender, key, accessList).isZero()) {
        count++;
      }
    }
    return count;
  }

  /**
   * Advances every selected keyed sequence past {@code nonceSeq}. The legacy set is consumed by the
   * caller incrementing the account nonce instead.
   *
   * @param updater the updater receiving the writes
   * @param sender the transaction sender
   * @param nonceKeys the transaction's nonce keys (not the legacy set)
   * @param nonceSeq the transaction's sequence
   * @param accessList the EIP-7928 access list recording the writes, if any
   */
  public static void consumeKeyed(
      final WorldUpdater updater,
      final Address sender,
      final List<UInt256> nonceKeys,
      final long nonceSeq,
      final Optional<? extends Eip7928AccessList> accessList) {
    final MutableAccount manager = updater.getOrCreate(NONCE_MANAGER);
    final UInt256 next = unsigned(nonceSeq + 1);
    for (final UInt256 key : nonceKeys) {
      final UInt256 slot = nonceSlot(sender, key);
      accessList.ifPresent(list -> list.addSlotAccessForAccount(NONCE_MANAGER, slot));
      manager.setStorageValue(slot, next);
    }
  }

  /**
   * The commitment to a key set exposed to the EVM: the keccak of the key count followed by every
   * key, each as a 32-byte big-endian word.
   *
   * @param nonceKeys the transaction's nonce keys
   * @return the hash
   */
  public static Bytes32 nonceKeysHash(final List<UInt256> nonceKeys) {
    final Bytes[] words = new Bytes[nonceKeys.size() + 1];
    words[0] = UInt256.valueOf(nonceKeys.size());
    for (int i = 0; i < nonceKeys.size(); i++) {
      words[i + 1] = nonceKeys.get(i);
    }
    return Bytes32.wrap(Hash.hash(Bytes.concatenate(words)).getBytes());
  }
}
