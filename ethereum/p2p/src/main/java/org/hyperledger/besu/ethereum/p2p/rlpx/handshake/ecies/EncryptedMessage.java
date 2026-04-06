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
package org.hyperledger.besu.ethereum.p2p.rlpx.handshake.ecies;

import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.crypto.SecureRandomProvider;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;

import java.nio.ByteBuffer;
import java.security.SecureRandom;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.MutableBytes;
import org.bouncycastle.crypto.InvalidCipherTextException;

final class EncryptedMessage {

  private static final int IV_SIZE = 16;

  private static final SecureRandom RANDOM = SecureRandomProvider.createSecureRandom();
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  private EncryptedMessage() {
    // Utility Class
  }

  /**
   * Decrypts the ciphertext using our private key.
   *
   * @param msgBytes The ciphertext.
   * @param nodeKey Abstraction of this nodes private key & associated cryptographic operations
   * @return The plaintext.
   * @throws InvalidCipherTextException Thrown if decryption failed.
   */
  public static Bytes decryptMsg(final Bytes msgBytes, final NodeKey nodeKey)
      throws InvalidCipherTextException {

    // Extract the ephemeral public key, stripping off the first byte (0x04), which designates it's
    // an uncompressed key.
    final SECPPublicKey ephPubKey = SIGNATURE_ALGORITHM.createPublicKey(msgBytes.slice(1, 64));

    // Strip off the IV to use.
    final Bytes iv = msgBytes.slice(65, IV_SIZE);

    // Extract the encrypted payload.
    final Bytes encrypted = msgBytes.slice(65 + IV_SIZE);

    // Perform the decryption.
    final ECIESEncryptionEngine decryptor =
        ECIESEncryptionEngine.forDecryption(nodeKey, ephPubKey, iv);
    return decryptor.decrypt(encrypted);
  }

  /**
   * Decrypts the ciphertext using our private key.
   *
   * @param msgBytes The ciphertext.
   * @param nodeKey Abstraction of this nodes private key & associated cryptographic operations
   * @return The plaintext.
   * @throws InvalidCipherTextException Thrown if decryption failed.
   */
  public static Bytes decryptMsgEIP8(final Bytes msgBytes, final NodeKey nodeKey)
      throws InvalidCipherTextException {
    final SECPPublicKey ephPubKey = SIGNATURE_ALGORITHM.createPublicKey(msgBytes.slice(3, 64));

    // Strip off the IV to use.
    final Bytes iv = msgBytes.slice(3 + 64, IV_SIZE);

    // Extract the encrypted payload.
    final Bytes encrypted = msgBytes.slice(3 + 64 + IV_SIZE);

    // Perform the decryption.
    final ECIESEncryptionEngine decryptor =
        ECIESEncryptionEngine.forDecryption(nodeKey, ephPubKey, iv);
    return decryptor.decrypt(encrypted, msgBytes.slice(0, 2).toArray());
  }

  /**
   * Encrypts a message for the specified peer using ECIES.
   *
   * @param bytes The plaintext.
   * @param remoteKey The peer's remote key.
   * @return The ciphertext.
   * @throws InvalidCipherTextException Thrown if encryption failed.
   */
  public static Bytes encryptMsg(final Bytes bytes, final SECPPublicKey remoteKey)
      throws InvalidCipherTextException {
    // TODO: check size.
    final ECIESEncryptionEngine engine = ECIESEncryptionEngine.forEncryption(remoteKey);

    // Do the encryption.
    final Bytes encrypted = engine.encrypt(bytes);
    final Bytes iv = engine.getIv();
    final SECPPublicKey ephPubKey = engine.getEphPubKey();

    // Create the output message by concatenating the ephemeral public key (prefixed with
    // 0x04 to designate uncompressed), IV, and encrypted bytes.
    final MutableBytes answer =
        MutableBytes.create(1 + ECIESHandshaker.PUBKEY_LENGTH + IV_SIZE + encrypted.size());

    int offset = 0;
    // Set the first byte as 0x04 to specify it's an uncompressed key.
    answer.set(offset, (byte) 0x04);
    ephPubKey.getEncodedBytes().copyTo(answer, offset += 1);
    iv.copyTo(answer, offset += ECIESHandshaker.PUBKEY_LENGTH);
    encrypted.copyTo(answer, offset + iv.size());
    return answer;
  }

  /**
   * Encrypts a message for the specified peer using ECIES.
   *
   * @param message The plaintext.
   * @param remoteKey The peer's remote key.
   * @return The ciphertext.
   * @throws InvalidCipherTextException Thrown if encryption failed.
   */
  public static Bytes encryptMsgEip8(final Bytes message, final SECPPublicKey remoteKey)
      throws InvalidCipherTextException {
    final ECIESEncryptionEngine engine = ECIESEncryptionEngine.forEncryption(remoteKey);

    // Do the encryption.
    final Bytes bytes = addPadding(message);
    final int size = bytes.size() + ECIESEncryptionEngine.ENCRYPTION_OVERHEAD;
    final byte[] sizePrefix = {(byte) (size >>> 8), (byte) size};
    final Bytes encrypted = engine.encrypt(bytes, sizePrefix);
    final Bytes iv = engine.getIv();
    final SECPPublicKey ephPubKey = engine.getEphPubKey();

    // Create the output message by concatenating the ephemeral public key (prefixed with
    // 0x04 to designate uncompressed), IV, and encrypted bytes.
    final MutableBytes answer =
        MutableBytes.create(3 + ECIESHandshaker.PUBKEY_LENGTH + IV_SIZE + encrypted.size());

    answer.set(0, sizePrefix[0]);
    answer.set(1, sizePrefix[1]);
    // Set the first byte as 0x04 to specify it's an uncompressed key.
    answer.set(2, (byte) 0x04);
    int offset = 0;
    ephPubKey.getEncodedBytes().copyTo(answer, offset += 3);
    iv.copyTo(answer, offset += ECIESHandshaker.PUBKEY_LENGTH);
    encrypted.copyTo(answer, offset + IV_SIZE);
    return answer;
  }

  private static Bytes addPadding(final Bytes message) {
    final byte[] raw = message.toArray();
    final int padding = 100 + RANDOM.nextInt(200);
    final byte[] paddingBytes = new byte[padding];
    RANDOM.nextBytes(paddingBytes);
    return Bytes.wrap(ByteBuffer.allocate(raw.length + padding).put(raw).put(paddingBytes).array());
  }
}
