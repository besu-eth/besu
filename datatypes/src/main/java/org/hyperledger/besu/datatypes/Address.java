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
package org.hyperledger.besu.datatypes;

import static com.google.common.base.Preconditions.checkArgument;
import static org.hyperledger.besu.crypto.Hash.keccak256;

import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.jspecify.annotations.Nullable;

/** A 160-bits account address. */
public class Address extends BytesHolder {

  /** The constant SIZE. */
  public static final int SIZE = 20;

  /** Specific addresses of the "precompiled" contracts. */
  public static final Address ECREC = Address.precompiled(0x01);

  /** The constant SHA256. */
  public static final Address SHA256 = Address.precompiled(0x02);

  /** The constant RIPEMD160. */
  public static final Address RIPEMD160 = Address.precompiled(0x03);

  /** The constant ID. */
  public static final Address ID = Address.precompiled(0x04);

  /** The constant MODEXP. */
  public static final Address MODEXP = Address.precompiled(0x05);

  /** The constant ALTBN128_ADD. */
  public static final Address ALTBN128_ADD = Address.precompiled(0x06);

  /** The constant ALTBN128_MUL. */
  public static final Address ALTBN128_MUL = Address.precompiled(0x07);

  /** The constant ALTBN128_PAIRING. */
  public static final Address ALTBN128_PAIRING = Address.precompiled(0x08);

  /** The constant BLAKE2B_F_COMPRESSION. */
  public static final Address BLAKE2B_F_COMPRESSION = Address.precompiled(0x09);

  /** The constant KZG_POINT_EVAL aka POINT_EVALUATION_PRECOMPILE_ADDRESS. */
  public static final Address KZG_POINT_EVAL = Address.precompiled(0xA);

  /** The constant BLS12_G1ADD. */
  public static final Address BLS12_G1ADD = Address.precompiled(0xB);

  /** The constant BLS12_G1MULTIEXP. */
  public static final Address BLS12_G1MULTIEXP = Address.precompiled(0xC);

  /** The constant BLS12_G2ADD. */
  public static final Address BLS12_G2ADD = Address.precompiled(0xD);

  /** The constant BLS12_G2MULTIEXP. */
  public static final Address BLS12_G2MULTIEXP = Address.precompiled(0xE);

  /** The constant BLS12_PAIRING. */
  public static final Address BLS12_PAIRING = Address.precompiled(0xF);

  /** The constant BLS12_MAP_FP_TO_G1. */
  public static final Address BLS12_MAP_FP_TO_G1 = Address.precompiled(0x10);

  /** The constant BLS12_MAP_FP2_TO_G2. */
  public static final Address BLS12_MAP_FP2_TO_G2 = Address.precompiled(0x11);

  /** Precompile address for P256_VERIFY. */
  public static final Address P256_VERIFY = Address.precompiled(0x0100);

  /** The constant ZERO. */
  public static final Address ZERO = Address.wrap(Bytes.fromHexStringLenient("0x0", SIZE));

  static LoadingCache<Address, Hash> hashCache =
      CacheBuilder.newBuilder()
          .maximumSize(4000)
          // .weakKeys() // unless we "intern" all addresses we cannot use weak or soft keys.
          .build(
              new CacheLoader<>() {
                @Override
                public Hash load(final Address key) {
                  return Hash.hash(key.getBytes());
                }
              });

  /**
   * Instantiates a new Address.
   *
   * @param bytes the bytes
   */
  protected Address(final Bytes bytes) {
    super(bytes);
  }

  /**
   * Wrap address.
   *
   * @param value the value
   * @return the address
   */
  public static Address wrap(final Bytes value) {
    checkArgument(
        value.size() == SIZE,
        "An account address must be %s bytes long, got %s",
        SIZE,
        value.size());
    return new Address(value);
  }

  /**
   * Creates an address from the given RLP-encoded input.
   *
   * @param input The input to read from
   * @return the input's corresponding address
   */
  public static Address readFrom(final RLPInput input) {
    final Bytes bytes = input.readBytes();
    if (bytes.size() != SIZE) {
      throw new RLPException(
          String.format("Address unexpected size of %s (needs %s)", bytes.size(), SIZE));
    }
    return Address.wrap(bytes);
  }

  /**
   * Extracts an address from a ECDSARECOVER result hash.
   *
   * @param hash A hash that has been obtained through hashing the return of the <code>
   *     ECDSARECOVER     </code> function from Appendix F (Signing Transactions) of the Ethereum
   *     Yellow Paper.
   * @return The ethereum address from the provided hash.
   */
  public static Address extract(final Bytes32 hash) {
    return wrap(hash.slice(12, 20));
  }

  /**
   * Extract address.
   *
   * @param publicKey the public key
   * @return the address
   */
  public static Address extract(final SECPPublicKey publicKey) {
    return Address.extract(keccak256(publicKey.getEncodedBytes()));
  }

  /**
   * Parse a hexadecimal string representing an account address.
   *
   * @param str A hexadecimal string (with or without the leading '0x') representing a valid account
   *     address.
   * @return The parsed address: {@code null} if the provided string is {@code null}.
   * @throws IllegalArgumentException if the string is either not hexadecimal, or not the valid
   *     representation of an address.
   */
  @JsonCreator
  public static @Nullable Address fromHexString(final String str) {
    if (str == null) return null;
    return wrap(Bytes.fromHexStringLenient(str, SIZE));
  }

  /**
   * Parse a hexadecimal string representing an account address.
   *
   * @param str A hexadecimal string representing a valid account address (strictly 20 bytes).
   * @return The parsed address.
   * @throws IllegalArgumentException if the provided string is {@code null}.
   * @throws IllegalArgumentException if the string is either not hexadecimal, or not the valid
   *     representation of a 20 byte address.
   */
  public static Address fromHexStringStrict(final String str) {
    checkArgument(str != null);
    final Bytes value = Bytes.fromHexString(str);
    checkArgument(
        value.size() == SIZE,
        "An account address must be %s bytes long, got %s",
        SIZE,
        value.size());
    return new Address(value);
  }

  /**
   * Precompiled address.
   *
   * @param value the value
   * @return the address
   */
  public static Address precompiled(final int value) {
    // Allow values up to 0x01FF (511) to encompass layer2 precompile address space
    checkArgument(value < 0x01FF, "Precompiled value must be <= 0x01FF");
    final byte[] address = new byte[SIZE];
    address[SIZE - 2] = (byte) (value >>> 8); // High byte
    address[SIZE - 1] = (byte) (value & 0xFF); // Low byte
    return new Address(Bytes.wrap(address));
  }

  /**
   * Address of the created contract.
   *
   * <p>This implement equation (86) in Section 7 of the Yellow Paper (rev. a91c29c).
   *
   * @param senderAddress the address of the transaction sender.
   * @param nonce the nonce of this transaction.
   * @return The generated address of the created contract.
   */
  public static Address contractAddress(final Address senderAddress, final long nonce) {
    return Address.extract(
        keccak256(
            RLP.encode(
                out -> {
                  out.startList();
                  out.writeBytes(senderAddress.getBytes());
                  out.writeLongScalar(nonce);
                  out.endList();
                })));
  }

  /**
   * Returns the hash of the address. Backed by a cache for performance reasons.
   *
   * @return the hash of the address.
   */
  public Hash addressHash() {
    try {
      return hashCache.get(this);
    } catch (ExecutionException e) {
      return Hash.hash(getBytes());
    }
  }
}
