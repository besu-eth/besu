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
package org.hyperledger.besu.cryptoservices;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NodeKeyTest {

  @BeforeEach
  public void resetInstance() {
    SignatureAlgorithmFactory.resetInstance();
  }

  @Test
  public void ecdhKeyAgreementCompressedIsSymmetric() {
    final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    final KeyPair kp1 = signatureAlgorithm.generateKeyPair();
    final KeyPair kp2 = signatureAlgorithm.generateKeyPair();

    final NodeKey nodeKey1 = new NodeKey(new KeyPairSecurityModule(kp1));
    final NodeKey nodeKey2 = new NodeKey(new KeyPairSecurityModule(kp2));

    final Bytes compressed1 = nodeKey1.calculateECDHKeyAgreementCompressed(kp2.getPublicKey());
    final Bytes compressed2 = nodeKey2.calculateECDHKeyAgreementCompressed(kp1.getPublicKey());

    assertThat(compressed1).isEqualTo(compressed2);
    assertThat(compressed1.size()).isEqualTo(33);
  }

  @Test
  public void ecdhCompressedXCoordinateMatchesUncompressed() {
    final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    final KeyPair kp1 = signatureAlgorithm.generateKeyPair();
    final KeyPair kp2 = signatureAlgorithm.generateKeyPair();

    final NodeKey nodeKey1 = new NodeKey(new KeyPairSecurityModule(kp1));

    final Bytes32 xOnly = nodeKey1.calculateECDHKeyAgreement(kp2.getPublicKey());
    final Bytes compressed = nodeKey1.calculateECDHKeyAgreementCompressed(kp2.getPublicKey());

    assertThat(compressed.slice(1, 32)).isEqualTo(xOnly);
  }

  @Test
  public void publicKeyShouldMatch() {
    final Bytes keyPairPubKey =
        Bytes.fromHexString(
            "0x35f48529f73c4172850ed18997d00d101a9aebc130103c23d41d46351c1b1d72ffc8b246af3a446d99a8218bd69e231b75932ea9a79173751dc7eb2eb0e301f2");
    final Bytes32 keyPairPrvKey =
        Bytes32.fromHexString("0xf7a58d5e755d51fa2f6206e91dd574597c73248aaf946ec1964b8c6268d6207b");

    final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    final KeyPair keyPair =
        signatureAlgorithm.createKeyPair(signatureAlgorithm.createPrivateKey(keyPairPrvKey));
    final KeyPairSecurityModule keyPairSecurityModule = new KeyPairSecurityModule(keyPair);
    final NodeKey nodeKey = new NodeKey(keyPairSecurityModule);

    assertThat(nodeKey.getPublicKey().getEncodedBytes())
        .isEqualByComparingTo(keyPair.getPublicKey().getEncodedBytes());

    assertThat(nodeKey.getPublicKey().getEncodedBytes()).isEqualByComparingTo(keyPairPubKey);
  }
}
