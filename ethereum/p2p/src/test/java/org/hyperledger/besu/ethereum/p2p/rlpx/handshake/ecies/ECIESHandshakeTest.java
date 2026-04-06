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
import io.netty.buffer.Unpooled;
import org.apache.tuweni.bytes.Bytes;
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

    // PyEVM
    private static final byte[] pyEvmInitiatorRqEnc =
        h("0x04a0274c5951e32132e7f088c9bdfdc76c9d91f0dc6078e848f8e3361193dbdc43b94351ea3d89e4ff33ddcefbc80070498824857f499656c4f79bbd97b6c51a514251d69fd1785ef8764bd1d262a883f780964cce6a14ff206daf1206aa073a2d35ce2697ebf3514225bef186631b2fd2316a4b7bcdefec8d75a1025ba2c5404a34e7795e1dd4bc01c6113ece07b0df13b69d3ba654a36e35e69ff9d482d88d2f0228e7d96fe11dccbb465a1831c7d4ad3a026924b182fc2bdfe016a6944312021da5cc459713b13b86a686cf34d6fe6615020e4acf26bf0d5b7579ba813e7723eb95b3cef9942f01a58bd61baee7c9bdd438956b426a4ffe238e61746a8c93d5e10680617c82e48d706ac4953f5e1c4c4f7d013c87d34a06626f498f34576dc017fdd3d581e83cfd26cf125b6d2bda1f1d56")
            .toArray();
  }

  // Messages.
  private static class Messages {

    // Encrypted messages -- we cannot assert against these because the Integrated Encryption Scheme
    // is designed to
    // use a different ephemeral key everytime.
    private static final byte[] initiatorMsgEnc =
        h("0x04a0274c5951e32132e7f088c9bdfdc76c9d91f0dc6078e848f8e3361193dbdc43b94351ea3d89e4ff33ddcefbc80070498824857f499656c4f79bbd97b6c51a514251d69fd1785ef8764bd1d262a883f780964cce6a14ff206daf1206aa073a2d35ce2697ebf3514225bef186631b2fd2316a4b7bcdefec8d75a1025ba2c5404a34e7795e1dd4bc01c6113ece07b0df13b69d3ba654a36e35e69ff9d482d88d2f0228e7d96fe11dccbb465a1831c7d4ad3a026924b182fc2bdfe016a6944312021da5cc459713b13b86a686cf34d6fe6615020e4acf26bf0d5b7579ba813e7723eb95b3cef9942f01a58bd61baee7c9bdd438956b426a4ffe238e61746a8c93d5e10680617c82e48d706ac4953f5e1c4c4f7d013c87d34a06626f498f34576dc017fdd3d581e83cfd26cf125b6d2bda1f1d56")
            .toArray();
    private static final byte[] responderMsgEnc =
        h("0x049934a7b2d7f9af8fd9db941d9da281ac9381b5740e1f64f7092f3588d4f87f5ce55191a6653e5e80c1c5dd538169aa123e70dc6ffc5af1827e546c0e958e42dad355bcc1fcb9cdf2cf47ff524d2ad98cbf275e661bf4cf00960e74b5956b799771334f426df007350b46049adb21a6e78ab1408d5e6ccde6fb5e69f0f4c92bb9c725c02f99fa72b9cdc8dd53cff089e0e73317f61cc5abf6152513cb7d833f09d2851603919bf0fbe44d79a09245c6e8338eb502083dc84b846f2fee1cc310d2cc8b1b9334728f97220bb799376233e113")
            .toArray();
  }

  private static class Expectations {

    private static final byte[] aesSecret =
        h32("0xc0458fa97a5230830e05f4f20b7c755c1d4e54b1ce5cf43260bb191eef4e418d").toArray();
    private static final byte[] macSecret =
        h32("0x48c938884d5067a1598272fcddaa4b833cd5e7d92e8228c0ecdfabbe68aef7f1").toArray();
    private static final byte[] token =
        h32("0x3f9ec2592d1554852b1f54d228f042ed0a9310ea86d038dc2b401ba8cd7fdac4").toArray();
    private static final byte[] initialEgressMac =
        h32("0x09771e93b1a6109e97074cbe2d2b0cf3d3878efafe68f53c41bb60c0ec49097e").toArray();
    private static final byte[] initialIngressMac =
        h32("0x75823d96e23136c89666ee025fb21a432be906512b3dd4a3049e898adb433847").toArray();
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

  @Test
  public void encryptedInitiationRqFromPyEvm() {
    // Create the responder handshaker.
    final ECIESHandshaker responder = new ECIESHandshaker();

    // Prepare the handshaker with the responder's keypair.
    responder.prepareResponder(NodeKeyUtils.createFrom(Input.responderKeyPair));

    // Set the test data.
    responder.setEphKeyPair(Input.responderEphKeyPair);
    responder.setResponderNonce(Input.responderNonce);

    // Pusht the request taken from PyEVM.
    final Optional<ByteBuf> responderMsg =
        responder.handleMessage(Unpooled.wrappedBuffer(Input.pyEvmInitiatorRqEnc));
    assertThat(responderMsg).isPresent();
    assertThat(responder.getStatus()).isEqualTo(HandshakeStatus.SUCCESS);
  }

  @Test
  public void ingressEgressMacsAsExpected() {
    // Initiator end of the handshake.
    final ECIESHandshaker initiator = new ECIESHandshaker();
    initiator.prepareInitiator(
        NodeKeyUtils.createFrom(Input.initiatorKeyPair), Input.responderKeyPair.getPublicKey());
    initiator.firstMessage();
    initiator.setInitiatorMsgEnc(Bytes.wrap(Messages.initiatorMsgEnc));
    initiator.setEphKeyPair(Input.initiatorEphKeyPair);
    initiator.setInitiatorNonce(Input.initiatorNonce);

    // Responder end of the handshake.
    final ECIESHandshaker responder = new ECIESHandshaker();
    responder.prepareResponder(NodeKeyUtils.createFrom(Input.responderKeyPair));
    responder.setEphKeyPair(Input.responderEphKeyPair);
    responder.setResponderNonce(Input.responderNonce);

    // Exchange encrypted test messages.
    responder.handleMessage(Unpooled.wrappedBuffer(Messages.initiatorMsgEnc));
    initiator.handleMessage(Unpooled.wrappedBuffer(Messages.responderMsgEnc));

    // Override the message sent from the responder with the test vector, and regenerate the
    // secrets.
    responder.setResponderMsgEnc(Bytes.wrap(Messages.responderMsgEnc));
    responder.computeSecrets();

    // Assert that the initiator's secrets match the expected values.
    assertThat(initiator.secrets().getAesSecret()).isEqualTo(Expectations.aesSecret);
    assertThat(initiator.secrets().getMacSecret()).isEqualTo(Expectations.macSecret);
    assertThat(initiator.secrets().getToken()).isEqualTo(Expectations.token);
    assertThat(initiator.secrets().getIngressMac()).isEqualTo(Expectations.initialIngressMac);
    assertThat(initiator.secrets().getEgressMac()).isEqualTo(Expectations.initialEgressMac);

    // Assert that the responder's secrets match the expected values, where the ingress and egress
    // are reversed.
    assertThat(responder.secrets().getAesSecret()).isEqualTo(Expectations.aesSecret);
    assertThat(responder.secrets().getMacSecret()).isEqualTo(Expectations.macSecret);
    assertThat(responder.secrets().getToken()).isEqualTo(Expectations.token);
    assertThat(responder.secrets().getIngressMac()).isEqualTo(Expectations.initialEgressMac);
    assertThat(responder.secrets().getEgressMac()).isEqualTo(Expectations.initialIngressMac);
  }

  private static Bytes h(final String hex) {
    return Bytes.fromHexString(hex);
  }

  private static Bytes32 h32(final String hex) {
    return Bytes32.fromHexString(hex);
  }
}
