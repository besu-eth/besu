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
import static org.apache.tuweni.bytes.Bytes.concatenate;
import static org.hyperledger.besu.crypto.Hash.keccak256;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.crypto.SecureRandomProvider;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.p2p.rlpx.handshake.HandshakeException;
import org.hyperledger.besu.ethereum.p2p.rlpx.handshake.HandshakeSecrets;
import org.hyperledger.besu.ethereum.p2p.rlpx.handshake.Handshaker;
import org.hyperledger.besu.plugin.services.securitymodule.SecurityModuleException;

import java.security.SecureRandom;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Elliptic Curve Integrated Encryption Scheme implementation, following the handshake ceremony
 * of Ethereum.
 *
 * @see <a href="https://github.com/ethereum/devp2p/blob/master/rlpx.md#encrypted-handshake">RLPx
 *     encrypted handshake</a>
 */
public class ECIESHandshaker implements Handshaker {

  private static final Logger LOG = LoggerFactory.getLogger(ECIESHandshaker.class);
  private static final SecureRandom RANDOM = SecureRandomProvider.publicSecureRandom();

  static final int PUBKEY_LENGTH = 64;

  // Keypairs under our control.
  private NodeKey nodeKey;
  private KeyPair ephKeyPair;

  // Party's material, only public keys.
  private SECPPublicKey partyPubKey;
  private SECPPublicKey partyEphPubKey;

  // Messages, for later MAC calculation.
  private InitiatorHandshakeMessage initiatorMsg;
  private ResponderHandshakeMessage responderMsg;
  private Bytes initiatorMsgEnc;
  private Bytes responderMsgEnc;

  // Nonces.
  private Bytes32 initiatorNonce;
  private Bytes32 responderNonce;

  // Whether we are the party who initiated this handshake or not.
  private boolean initiator;

  // See Javadoc on #secrets() to understand the state machine.
  private final AtomicReference<Handshaker.HandshakeStatus> status =
      new AtomicReference<>(Handshaker.HandshakeStatus.UNINITIALIZED);
  private HandshakeSecrets secrets;

  private final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();

  @Override
  public void prepareInitiator(final NodeKey nodeKey, final SECPPublicKey theirPubKey) {
    checkState(
        status.compareAndSet(
            Handshaker.HandshakeStatus.UNINITIALIZED, Handshaker.HandshakeStatus.PREPARED),
        "handshake was already prepared");

    this.initiator = true;
    this.nodeKey = nodeKey;
    this.ephKeyPair = signatureAlgorithm.generateKeyPair();
    this.partyPubKey = theirPubKey;
    this.initiatorNonce = Bytes32.wrap(random(32), 0);
    LOG.trace(
        "Prepared ECIES handshake with node {}... under INITIATOR role",
        theirPubKey.getEncodedBytes().slice(0, 16));
  }

  @Override
  public void prepareResponder(final NodeKey nodeKey) {
    checkState(
        status.compareAndSet(
            Handshaker.HandshakeStatus.UNINITIALIZED, Handshaker.HandshakeStatus.IN_PROGRESS),
        "handshake was already prepared");

    this.initiator = false;
    this.nodeKey = nodeKey;
    this.ephKeyPair = signatureAlgorithm.generateKeyPair();
    this.responderNonce = Bytes32.wrap(random(32), 0);
    LOG.trace("Prepared ECIES handshake under RESPONDER role");
  }

  @Override
  public ByteBuf firstMessage() throws HandshakeException {
    checkState(initiator, "illegal invocation of firstMessage on non-initiator end of handshake");
    checkState(
        status.compareAndSet(
            Handshaker.HandshakeStatus.PREPARED, Handshaker.HandshakeStatus.IN_PROGRESS),
        "illegal invocation of firstMessage, handshake had already started");

    final Bytes32 staticSharedSecret = nodeKey.calculateECDHKeyAgreement(partyPubKey);
    initiatorMsg =
        InitiatorHandshakeMessageV4.create(
            nodeKey.getPublicKey(), ephKeyPair, staticSharedSecret, initiatorNonce);
    try {
      initiatorMsgEnc = EncryptedMessage.encryptMsgEip8(initiatorMsg.encode(), partyPubKey);
    } catch (final InvalidCipherTextException e) {
      status.set(Handshaker.HandshakeStatus.FAILED);
      throw new HandshakeException("Encrypting the first handshake message failed", e);
    }

    LOG.trace("First ECIES handshake message under INITIATOR role: {}", initiatorMsg);

    return Unpooled.wrappedBuffer(initiatorMsgEnc.toArray());
  }

  @Override
  public Optional<ByteBuf> handleMessage(final ByteBuf buf) throws HandshakeException {
    checkState(
        status.get() == Handshaker.HandshakeStatus.IN_PROGRESS,
        "illegal invocation of onMessage on handshake that is not in progress");

    // Read the EIP-8 size prefix to determine the full message length.
    buf.markReaderIndex();
    if (buf.readableBytes() < 2) {
      return Optional.empty();
    }
    final int size = buf.readUnsignedShort();
    if (size > buf.readableBytes()) {
      buf.resetReaderIndex();
      return Optional.empty();
    }

    // Read the full EIP-8 message (size prefix + payload).
    buf.resetReaderIndex();
    final byte[] fullMessage = new byte[size + 2];
    buf.readBytes(fullMessage);
    final Bytes encryptedMsg = Bytes.wrap(fullMessage);

    final Bytes bytes;
    try {
      bytes = EncryptedMessage.decryptMsgEIP8(encryptedMsg, nodeKey);
    } catch (final InvalidCipherTextException e) {
      status.set(Handshaker.HandshakeStatus.FAILED);
      throw new HandshakeException("Decrypting an incoming handshake message failed", e);
    } catch (final SecurityModuleException e) {
      status.set(Handshaker.HandshakeStatus.FAILED);
      throw new HandshakeException(
          "Unable to create ECDH Key agreement due to Crypto engine failure", e);
    }

    Optional<Bytes> nextMsg = Optional.empty();
    if (initiator) {
      // If we are the initiator, we have already sent our request and we're waiting for the
      // responder's ack;
      // when we receive it, we can build the handshake secret material and declare a SUCCESS.
      checkState(
          responderMsg == null,
          "unexpected message: responder message had " + "already been received");

      // Store the message, as we need it to generating our ingress and egress MACs.
      responderMsgEnc = encryptedMsg;
      responderMsg = ResponderHandshakeMessageV4.decode(bytes);

      // Extract the responder's nonce and ephemeral pubkey, which will be used to generate the
      // shared secrets.
      responderNonce = responderMsg.getNonce();
      partyEphPubKey = responderMsg.getEphPublicKey();

      LOG.trace(
          "Received responder's ECIES handshake message from node {}...: {}",
          partyPubKey.getEncodedBytes().slice(0, 16),
          responderMsg);

    } else {
      // If we are the responder, we are waiting for an initiator message; after we generate our
      // message and
      // we can build the handshake secret material and declare a SUCCESS.
      checkState(
          initiatorMsg == null,
          "unexpected message: initiator message " + "had already been received");

      // Store the message, as we need it to generating our ingress and egress MACs.
      initiatorMsgEnc = encryptedMsg;
      try {
        initiatorMsg = InitiatorHandshakeMessageV4.decode(bytes, nodeKey);
      } catch (final SecurityModuleException e) {
        status.set(Handshaker.HandshakeStatus.FAILED);
        throw new HandshakeException(
            "Unable to create ECDH Key agreement due to Crypto engine failure", e);
      }

      LOG.trace(
          "[{}] Received initiator's ECIES handshake message: {}",
          nodeKey.getPublicKey().getEncodedBytes(),
          initiatorMsg);

      // Extract the initiator's data.
      initiatorNonce = initiatorMsg.getNonce();
      partyPubKey = initiatorMsg.getPubKey();
      partyEphPubKey = initiatorMsg.getEphPubKey();

      checkState(
          keccak256(partyEphPubKey.getEncodedBytes()).equals(initiatorMsg.getEphPubKeyHash()),
          "keccak hash of recovered ephemeral pubkey does not match announced hash");

      // Build the response message.
      responderMsg = ResponderHandshakeMessageV4.create(ephKeyPair.getPublicKey(), responderNonce);

      LOG.trace(
          "Generated responder's ECIES handshake message against peer {}...: {}",
          partyPubKey.getEncodedBytes().slice(0, 16),
          responderMsg);

      try {
        responderMsgEnc = EncryptedMessage.encryptMsgEip8(responderMsg.encode(), partyPubKey);
      } catch (final InvalidCipherTextException e) {
        status.set(Handshaker.HandshakeStatus.FAILED);
        throw new HandshakeException("Encrypting the next handshake message failed", e);
      }
      nextMsg = Optional.of(responderMsgEnc);

      // Compute the secrets and declare this handshake as successful.
    }

    try {
      computeSecrets();
    } catch (final SecurityModuleException e) {
      status.set(Handshaker.HandshakeStatus.FAILED);
      throw new HandshakeException(
          "Unable to create ECDH Key agreement due to Crypto engine failure", e);
    }

    status.set(Handshaker.HandshakeStatus.SUCCESS);
    LOG.trace("Handshake status set to {}", status.get());
    return nextMsg.map(bv -> Unpooled.wrappedBuffer(bv.toArray()));
  }

  /**
   * Returns the current status of this handshake.
   *
   * <p>Starts {@link Handshaker.HandshakeStatus#UNINITIALIZED} and moves to {@link
   * Handshaker.HandshakeStatus#PREPARED} when a prepared* method is called, or to {@link
   * Handshaker.HandshakeStatus#IN_PROGRESS} if we're the responder part and have nothing to prepare
   * since we're awaiting the initiator's message.
   *
   * <p>As soon as we receive the expected message, the status transitions to {@link
   * Handshaker.HandshakeStatus#SUCCESS} if the message is well formed and we're able to generate
   * the resulting secrets.
   *
   * @return Returns the current status of this handshake.
   */
  @Override
  public Handshaker.HandshakeStatus getStatus() {
    return status.get();
  }

  @Override
  public HandshakeSecrets secrets() {
    checkState(
        status.get() == Handshaker.HandshakeStatus.SUCCESS,
        "cannot obtain secrets from an unsuccessful handshake");
    return secrets;
  }

  @Override
  public SECPPublicKey partyPubKey() {
    checkState(
        initiator || status.get() == Handshaker.HandshakeStatus.SUCCESS,
        "under the role of responder, cannot return the party's public "
            + "key until the handshake has completed");
    return partyPubKey;
  }

  /** Computes the secrets from the two exchanged messages. */
  void computeSecrets() {
    final Bytes agreedSecret =
        signatureAlgorithm.calculateECDHKeyAgreement(ephKeyPair.getPrivateKey(), partyEphPubKey);

    final Bytes sharedSecret =
        keccak256(
            concatenate(agreedSecret, keccak256(concatenate(responderNonce, initiatorNonce))));

    final Bytes32 aesSecret = keccak256(concatenate(agreedSecret, sharedSecret));
    final Bytes32 macSecret = keccak256(concatenate(agreedSecret, aesSecret));
    final Bytes32 token = keccak256(sharedSecret);

    final HandshakeSecrets secrets =
        new HandshakeSecrets(aesSecret.toArray(), macSecret.toArray(), token.toArray());

    final Bytes initiatorMac = concatenate(macSecret.xor(responderNonce), initiatorMsgEnc);
    final Bytes responderMac = concatenate(macSecret.xor(initiatorNonce), responderMsgEnc);

    if (initiator) {
      secrets.updateEgress(initiatorMac.toArray());
      secrets.updateIngress(responderMac.toArray());
    } else {
      secrets.updateIngress(initiatorMac.toArray());
      secrets.updateEgress(responderMac.toArray());
    }

    this.secrets = secrets;
  }

  static Bytes random(final int size) {
    final byte[] iv = new byte[size];
    RANDOM.nextBytes(iv);
    return Bytes.wrap(iv);
  }

  // ---------------------------------------------
  //  The methods below are for testing purposes.
  // ---------------------------------------------

  @VisibleForTesting
  NodeKey getNodeKey() {
    return nodeKey;
  }

  @VisibleForTesting
  KeyPair getEphKeyPair() {
    return ephKeyPair;
  }

  @VisibleForTesting
  void setEphKeyPair(final KeyPair ephKeyPair) {
    this.ephKeyPair = ephKeyPair;
  }

  @VisibleForTesting
  SECPPublicKey getPartyEphPubKey() {
    return partyEphPubKey;
  }

  @VisibleForTesting
  Bytes32 getInitiatorNonce() {
    return initiatorNonce;
  }

  @VisibleForTesting
  void setInitiatorNonce(final Bytes32 initiatorNonce) {
    this.initiatorNonce = initiatorNonce;
  }

  @VisibleForTesting
  Bytes32 getResponderNonce() {
    return responderNonce;
  }

  @VisibleForTesting
  void setResponderNonce(final Bytes32 responderNonce) {
    this.responderNonce = responderNonce;
  }
}
