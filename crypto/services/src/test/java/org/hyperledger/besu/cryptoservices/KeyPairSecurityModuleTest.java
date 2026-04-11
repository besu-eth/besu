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

import org.hyperledger.besu.crypto.ECPointUtil;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.KeyPairUtil;
import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.security.spec.ECPoint;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class KeyPairSecurityModuleTest {

  @TempDir public Path keyFile;

  @Test
  public void ecdhKeyAgreementCompressedIsSymmetric() throws IOException {
    final KeyPair kp1 = KeyPairUtil.loadKeyPair(keyFile.resolve("key1"));
    final KeyPair kp2 = KeyPairUtil.loadKeyPair(keyFile.resolve("key2"));

    final KeyPairSecurityModule module1 = new KeyPairSecurityModule(kp1);
    final KeyPairSecurityModule module2 = new KeyPairSecurityModule(kp2);

    final Bytes compressed1 =
        module1.calculateECDHKeyAgreementCompressed(
            () ->
                ECPointUtil.fromBouncyCastleECPoint(
                    SignatureAlgorithmFactory.getInstance()
                        .publicKeyAsEcPoint(kp2.getPublicKey())));
    final Bytes compressed2 =
        module2.calculateECDHKeyAgreementCompressed(
            () ->
                ECPointUtil.fromBouncyCastleECPoint(
                    SignatureAlgorithmFactory.getInstance()
                        .publicKeyAsEcPoint(kp1.getPublicKey())));

    Assertions.assertThat(compressed1).isEqualTo(compressed2);
    Assertions.assertThat(compressed1.size()).isEqualTo(33);
  }

  @Test
  public void ecdhKeyAgreementCompressedXCoordinateMatchesUncompressed() throws IOException {
    final KeyPair kp1 = KeyPairUtil.loadKeyPair(keyFile.resolve("key1"));
    final KeyPair kp2 = KeyPairUtil.loadKeyPair(keyFile.resolve("key2"));

    final KeyPairSecurityModule module1 = new KeyPairSecurityModule(kp1);
    final var partyKey =
        ECPointUtil.fromBouncyCastleECPoint(
            SignatureAlgorithmFactory.getInstance().publicKeyAsEcPoint(kp2.getPublicKey()));

    final Bytes32 xOnly = module1.calculateECDHKeyAgreement(() -> partyKey);
    final Bytes compressed = module1.calculateECDHKeyAgreementCompressed(() -> partyKey);

    Assertions.assertThat(compressed.slice(1, 32)).isEqualTo(xOnly);
  }

  @Test
  public void validatePublicKeyFromECPointCanBeConstructed() throws IOException {
    final KeyPair keyPair = KeyPairUtil.loadKeyPair(keyFile.resolve("key"));

    final KeyPairSecurityModule keyPairSecurityModule = new KeyPairSecurityModule(keyPair);
    final ECPoint ecPoint = keyPairSecurityModule.getPublicKey().getW();
    final Bytes encodedBytes = ECPointUtil.getEncodedBytes(ecPoint);
    final SECPPublicKey publicKey =
        SignatureAlgorithmFactory.getInstance().createPublicKey(encodedBytes);

    Assertions.assertThat(keyPair.getPublicKey().getEncodedBytes())
        .isEqualByComparingTo(publicKey.getEncodedBytes());
  }
}
