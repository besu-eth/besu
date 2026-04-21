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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.ethereum.p2p.rlpx.handshake.Handshaker.HandshakeStatus;

import java.util.Optional;

import io.netty.buffer.ByteBuf;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/** Test vectors taken from https://gist.github.com/fjl/3a78780d17c755d22df2 */
public class ECIESHandshakeTest {

  // Input data.
  private static class Input {

    private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
        SignatureAlgorithmFactory.getInstance();

    // Keys.
    private static final KeyPair initiatorKeyPair =
        SIGNATURE_ALGORITHM.createKeyPair(
            SIGNATURE_ALGORITHM.createPrivateKey(
                h32("0x5e173f6ac3c669587538e7727cf19b782a4f2fda07c1eaa662c593e5e85e3051")));
    private static final KeyPair initiatorEphKeyPair =
        SIGNATURE_ALGORITHM.createKeyPair(
            SIGNATURE_ALGORITHM.createPrivateKey(
                h32("0x19c2185f4f40634926ebed3af09070ca9e029f2edd5fae6253074896205f5f6c")));
    private static final KeyPair responderKeyPair =
        SIGNATURE_ALGORITHM.createKeyPair(
            SIGNATURE_ALGORITHM.createPrivateKey(
                h32("0xc45f950382d542169ea207959ee0220ec1491755abe405cd7498d6b16adb6df8")));
    private static final KeyPair responderEphKeyPair =
        SIGNATURE_ALGORITHM.createKeyPair(
            SIGNATURE_ALGORITHM.createPrivateKey(
                h32("0xd25688cf0ab10afa1a0e2dba7853ed5f1e5bf1c631757ed4e103b593ff3f5620")));

    // Nonces.
    private static final Bytes32 initiatorNonce =
        h32("0xcd26fecb93657d1cd9e9eaf4f8be720b56dd1d39f190c4e1c6b7ec66f077bb11");
    private static final Bytes32 responderNonce =
        h32("0xf37ec61d84cea03dcc5e8385db93248584e8af4b4d1c832d8c7453c0089687a7");
  }

  @Test
  public void authPlainTextWithEncryption() {
    // Start a handshaker disabling encryption.
    final ECIESHandshaker initiator = new ECIESHandshaker();

    // Prepare the handshaker to take the initiator role.
    initiator.prepareInitiator(
        NodeKeyUtils.createFrom(Input.initiatorKeyPair), Input.responderKeyPair.getPublicKey());

    // Set the test vectors.
    initiator.setEphKeyPair(Input.initiatorEphKeyPair);
    initiator.setInitiatorNonce(Input.initiatorNonce);

    // Get the first message and compare it against expected output value.
    final ByteBuf initiatorRq = initiator.firstMessage();
    // assertThat(initiatorRq).isEqualTo(Messages.initiatorMsgPlain);

    // Create the responder handshaker.
    final ECIESHandshaker responder = new ECIESHandshaker();

    // Prepare the handshaker with the responder's keypair.
    responder.prepareResponder(NodeKeyUtils.createFrom(Input.responderKeyPair));

    // Set the test data.
    responder.setEphKeyPair(Input.responderEphKeyPair);
    responder.setResponderNonce(Input.responderNonce);

    // Give the responder the initiator's request. Check that it has a message to send.
    final ByteBuf responderRp =
        responder
            .handleMessage(initiatorRq)
            .orElseThrow(() -> new AssertionError("Expected responder message"));
    assertThat(responder.getPartyEphPubKey()).isEqualTo(initiator.getEphKeyPair().getPublicKey());
    assertThat(responder.getInitiatorNonce()).isEqualTo(initiator.getInitiatorNonce());
    assertThat(responder.partyPubKey()).isEqualTo(initiator.getNodeKey().getPublicKey());

    // Provide that message to the initiator, check that it has nothing to send.
    final Optional<ByteBuf> noMessage = initiator.handleMessage(responderRp);
    assertThat(noMessage).isNotPresent();

    // Ensure that both handshakes are in SUCCESS state.
    assertThat(initiator.getStatus()).isEqualTo(HandshakeStatus.SUCCESS);
    assertThat(responder.getStatus()).isEqualTo(HandshakeStatus.SUCCESS);

    // Compare data across handshakes.
    assertThat(initiator.getPartyEphPubKey()).isEqualTo(responder.getEphKeyPair().getPublicKey());
    assertThat(initiator.getResponderNonce()).isEqualTo(Input.responderNonce);
  }

  private static Bytes32 h32(final String hex) {
    return Bytes32.fromHexString(hex);
  }
}
