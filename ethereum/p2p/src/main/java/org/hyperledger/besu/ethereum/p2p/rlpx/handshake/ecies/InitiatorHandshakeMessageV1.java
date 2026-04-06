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

import static com.google.common.base.Preconditions.checkState;

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;

/**
 * The initiator's handshake message.
 *
 * <p>This message must be sent by the party that initiates the RLPX connection, as the first
 * message in the handshake protocol.
 *
 * <h3>Message structure</h3>
 *
 * The following describes the message structure:
 *
 * <pre>
 *   authInitiator -&gt; E(remote-pubk,
 *                      S(ephemeral-privk, static-shared-secret ^ nonce)
 *                       || H(ephemeral-pubk)
 *                      || pubk
 *                      || nonce
 *                      || 0x0)
 * </pre>
 *
 * @see <a href=
 *     "https://github.com/ethereum/devp2p/blob/master/rlpx.md#encrypted-handshake">Structure of the
 *     initiator request</a>
 */
public final class InitiatorHandshakeMessageV1 implements InitiatorHandshakeMessage {
  public static final int MESSAGE_LENGTH =
      ECIESHandshaker.SIGNATURE_LENGTH
          + ECIESHandshaker.HASH_EPH_PUBKEY_LENGTH
          + ECIESHandshaker.PUBKEY_LENGTH
          + ECIESHandshaker.NONCE_LENGTH
          + ECIESHandshaker.TOKEN_FLAG_LENGTH;

  private final SECPPublicKey pubKey;
  private final SECPSignature signature;
  private final SECPPublicKey ephPubKey;
  private final Bytes32 ephPubKeyHash;
  private final Bytes32 nonce;
  private final boolean token;

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  InitiatorHandshakeMessageV1(
      final SECPPublicKey pubKey,
      final SECPSignature signature,
      final SECPPublicKey ephPubKey,
      final Bytes32 ephPubKeyHash,
      final Bytes32 nonce,
      final boolean token) {
    this.pubKey = pubKey;
    this.signature = signature;
    this.ephPubKey = ephPubKey;
    this.ephPubKeyHash = ephPubKeyHash;
    this.nonce = nonce;
    this.token = token;
  }

  public static InitiatorHandshakeMessageV1 create(
      final SECPPublicKey ourPubKey,
      final KeyPair ephKeyPair,
      final Bytes32 staticSharedSecret,
      final Bytes32 nonce,
      final boolean token) {
    final Bytes32 ephPubKeyHash = Hash.keccak256(ephKeyPair.getPublicKey().getEncodedBytes());

    // XOR of the static shared secret and the generated nonce.
    final SECPSignature signature =
        SIGNATURE_ALGORITHM.sign(staticSharedSecret.xor(nonce), ephKeyPair);
    return new InitiatorHandshakeMessageV1(
        ourPubKey, signature, ephKeyPair.getPublicKey(), ephPubKeyHash, nonce, token);
  }

  /**
   * Decodes this message.
   *
   * @param bytes The raw bytes.
   * @param nodeKey The nodeKey used to calculate ECDH key agreements.
   * @return The decoded message.
   */
  public static InitiatorHandshakeMessageV1 decode(final Bytes bytes, final NodeKey nodeKey) {
    checkState(bytes.size() == MESSAGE_LENGTH);

    int offset = 0;
    final SECPSignature signature =
        SIGNATURE_ALGORITHM.decodeSignature(bytes.slice(offset, ECIESHandshaker.SIGNATURE_LENGTH));
    final Bytes32 ephPubKeyHash =
        Bytes32.wrap(
            bytes.slice(
                offset += ECIESHandshaker.SIGNATURE_LENGTH, ECIESHandshaker.HASH_EPH_PUBKEY_LENGTH),
            0);
    final SECPPublicKey pubKey =
        SIGNATURE_ALGORITHM.createPublicKey(
            bytes.slice(
                offset += ECIESHandshaker.HASH_EPH_PUBKEY_LENGTH, ECIESHandshaker.PUBKEY_LENGTH));
    final Bytes32 nonce =
        Bytes32.wrap(
            bytes.slice(offset += ECIESHandshaker.PUBKEY_LENGTH, ECIESHandshaker.NONCE_LENGTH), 0);
    final boolean token = bytes.get(offset) == 0x01;

    final Bytes32 staticSharedSecret = nodeKey.calculateECDHKeyAgreement(pubKey);
    final SECPPublicKey ephPubKey =
        SIGNATURE_ALGORITHM
            .recoverPublicKeyFromSignature(staticSharedSecret.xor(nonce), signature)
            .orElseThrow(() -> new RuntimeException("Could not recover public key from signature"));

    return new InitiatorHandshakeMessageV1(
        pubKey, signature, ephPubKey, ephPubKeyHash, nonce, token);
  }

  @Override
  public Bytes encode() {
    final MutableBytes bytes = MutableBytes.create(MESSAGE_LENGTH);
    signature.encodedBytes().copyTo(bytes, 0);
    ephPubKeyHash.copyTo(bytes, ECIESHandshaker.SIGNATURE_LENGTH);
    pubKey
        .getEncodedBytes()
        .copyTo(bytes, ECIESHandshaker.SIGNATURE_LENGTH + ECIESHandshaker.HASH_EPH_PUBKEY_LENGTH);
    nonce.copyTo(
        bytes,
        ECIESHandshaker.SIGNATURE_LENGTH
            + ECIESHandshaker.HASH_EPH_PUBKEY_LENGTH
            + ECIESHandshaker.PUBKEY_LENGTH);
    bytes.set(MESSAGE_LENGTH - 1, (byte) (token ? 0x01 : 0x00));
    return bytes;
  }

  @Override
  public SECPPublicKey getPubKey() {
    return pubKey;
  }

  @Override
  public Bytes32 getEphPubKeyHash() {
    return ephPubKeyHash;
  }

  @Override
  public Bytes32 getNonce() {
    return nonce;
  }

  @Override
  public SECPPublicKey getEphPubKey() {
    return ephPubKey;
  }

  @Override
  public String toString() {
    return "InitiatorHandshakeMessage{"
        + "pubKey="
        + pubKey
        + ", signature="
        + signature
        + ", ephPubKey="
        + ephPubKey
        + ", ephPubKeyHash="
        + ephPubKeyHash
        + ", nonce="
        + nonce
        + ", token="
        + token
        + '}';
  }
}
