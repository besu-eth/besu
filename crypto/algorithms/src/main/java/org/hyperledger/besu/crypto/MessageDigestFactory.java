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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Security;

import org.bouncycastle.jcajce.provider.digest.Keccak;
import org.bouncycastle.jcajce.provider.digest.RIPEMD160;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

/** The Message digest factory. */
public class MessageDigestFactory {

  private MessageDigestFactory() {}

  /** Keccak-256 */
  public static final String KECCAK256_ALG = "KECCAK-256";

  /** SHA-256 */
  public static final String SHA256_ALG = "SHA-256";

  /** RipeMD-160 */
  public static final String RIPEMD160_ALG = "RIPEMD160";

  /** Blake2b F Function */
  public static final String BLAKE2BF_ALG = "BLAKE2BF";

  static {
    Security.addProvider(new BesuProvider());
    Security.addProvider(new BouncyCastleProvider());
  }

  /**
   * Create message digest.
   *
   * @param algorithm the algorithm
   * @return the message digest
   * @throws NoSuchAlgorithmException the no such algorithm exception
   */
  @SuppressWarnings("DoNotInvokeMessageDigestDirectly")
  public static MessageDigest create(final String algorithm) throws NoSuchAlgorithmException {
    return switch (algorithm) {
      case KECCAK256_ALG -> new Keccak.Digest256();
      case RIPEMD160_ALG -> new RIPEMD160.Digest();
      case BLAKE2BF_ALG -> new Blake2bfMessageDigest();
      // SHA-256 (and other standard algorithms) resolved via JCA provider priority order
      // enabling SHA-NI hardware acceleration if available
      default -> MessageDigest.getInstance(algorithm);
    };
  }
}
