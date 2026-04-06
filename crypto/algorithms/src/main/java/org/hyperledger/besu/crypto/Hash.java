/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.crypto;

import static org.hyperledger.besu.crypto.MessageDigestFactory.BLAKE2BF_ALG;
import static org.hyperledger.besu.crypto.MessageDigestFactory.KECCAK256_ALG;
import static org.hyperledger.besu.crypto.MessageDigestFactory.RIPEMD160_ALG;
import static org.hyperledger.besu.crypto.MessageDigestFactory.SHA256_ALG;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Various utilities for providing hashes (digests) of arbitrary data. */
public abstract class Hash {
  private Hash() {}

  private static final Supplier<MessageDigest> KECCAK256_SUPPLIER =
      Suppliers.memoize(() -> messageDigest(KECCAK256_ALG));
  private static final ThreadLocal<MessageDigest> SHA256_DIGEST =
      ThreadLocal.withInitial(() -> messageDigest(SHA256_ALG));
  private static final Supplier<MessageDigest> RIPEMD160_SUPPLIER =
      Suppliers.memoize(() -> messageDigest(RIPEMD160_ALG));
  private static final Supplier<MessageDigest> BLAKE2BF_SUPPLIER =
      Suppliers.memoize(() -> messageDigest(BLAKE2BF_ALG));

  private static MessageDigest messageDigest(final String algorithm) {
    try {
      return MessageDigestFactory.create(algorithm);
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Helper method to generate a digest using the provided algorithm.
   *
   * @param input The input bytes to produce the digest for.
   * @param digestSupplier the digest supplier to use
   * @return A digest.
   */
  private static byte[] digestUsingAlgorithm(
      final Bytes input, final Supplier<MessageDigest> digestSupplier) {
    try {
      final MessageDigest digest = (MessageDigest) digestSupplier.get().clone();
      input.update(digest);
      return digest.digest();
    } catch (final CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Digest using SHA2-256.
   *
   * @param input The input bytes to produce the digest for.
   * @return A digest.
   */
  public static Bytes32 sha256(final Bytes input) {
    final MessageDigest digest = SHA256_DIGEST.get();
    input.update(digest);
    return Bytes32.wrap(digest.digest());
  }

  /**
   * Digest using keccak-256.
   *
   * @param input The input bytes to produce the digest for.
   * @return A digest.
   */
  public static Bytes32 keccak256(final Bytes input) {
    return Bytes32.wrap(digestUsingAlgorithm(input, KECCAK256_SUPPLIER));
  }

  /**
   * Digest using RIPEMD-160.
   *
   * @param input The input bytes to produce the digest for.
   * @return A digest.
   */
  public static Bytes ripemd160(final Bytes input) {
    return Bytes.wrap(digestUsingAlgorithm(input, RIPEMD160_SUPPLIER));
  }

  /**
   * Digest using Blake2f compression function.
   *
   * @param input The input bytes to produce the digest for.
   * @return A digest.
   */
  public static Bytes blake2bf(final Bytes input) {
    return Bytes.wrap(digestUsingAlgorithm(input, BLAKE2BF_SUPPLIER));
  }
}
