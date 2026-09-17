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

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public final class InitiatorHandshakeMessageV4 implements InitiatorHandshakeMessage {

  public static final int VERSION = 4;
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  private final SECPPublicKey pubKey;
  private final SECPSignature signature;
  private final SECPPublicKey ephPubKey;
  private final Bytes32 ephPubKeyHash;
  private final Bytes32 nonce;

  public static InitiatorHandshakeMessageV4 create(
      final SECPPublicKey ourPubKey,
      final KeyPair ephKeyPair,
      final Bytes32 staticSharedSecret,
      final Bytes32 nonce) {
    return new InitiatorHandshakeMessageV4(
        ourPubKey,
        SIGNATURE_ALGORITHM.sign(staticSharedSecret.xor(nonce), ephKeyPair),
        ephKeyPair.getPublicKey(),
        nonce);
  }

  /**
   * Decodes this message.
   *
   * @param bytes The raw bytes.
   * @param nodeKey Abstraction of the local nodes keys and associated cryptographic operations
   * @return The decoded message.
   */
  public static InitiatorHandshakeMessageV4 decode(final Bytes bytes, final NodeKey nodeKey) {
    final RLPInput input = new BytesValueRLPInput(bytes, true);
    input.enterList();
    final SECPSignature signature = SIGNATURE_ALGORITHM.decodeSignature(input.readBytes());
    final SECPPublicKey pubKey = SIGNATURE_ALGORITHM.createPublicKey(input.readBytes());
    final Bytes32 nonce = input.readBytes32();
    final Bytes32 staticSharedSecret = nodeKey.calculateECDHKeyAgreement(pubKey);
    final SECPPublicKey ephPubKey =
        SIGNATURE_ALGORITHM
            .recoverPublicKeyFromSignature(staticSharedSecret.xor(nonce), signature)
            .orElseThrow(() -> new RuntimeException("Could not recover public key from signature"));

    return new InitiatorHandshakeMessageV4(pubKey, signature, ephPubKey, nonce);
  }

  private InitiatorHandshakeMessageV4(
      final SECPPublicKey pubKey,
      final SECPSignature signature,
      final SECPPublicKey ephPubKey,
      final Bytes32 nonce) {
    this.pubKey = pubKey;
    this.signature = signature;
    this.ephPubKey = ephPubKey;
    this.ephPubKeyHash = Hash.keccak256(ephPubKey.getEncodedBytes());
    this.nonce = nonce;
  }

  @Override
  public Bytes encode() {
    final BytesValueRLPOutput temp = new BytesValueRLPOutput();
    temp.startList();
    temp.writeBytes(signature.encodedBytes());
    temp.writeBytes(pubKey.getEncodedBytes());
    temp.writeBytes(nonce);
    temp.writeIntScalar(VERSION);
    temp.endList();
    return temp.encoded();
  }

  @Override
  public Bytes32 getNonce() {
    return nonce;
  }

  @Override
  public SECPPublicKey getPubKey() {
    return pubKey;
  }

  @Override
  public SECPPublicKey getEphPubKey() {
    return ephPubKey;
  }

  @Override
  public Bytes32 getEphPubKeyHash() {
    return ephPubKeyHash;
  }
}
