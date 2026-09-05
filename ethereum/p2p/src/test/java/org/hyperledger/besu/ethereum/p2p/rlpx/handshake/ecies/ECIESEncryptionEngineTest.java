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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;

import org.apache.tuweni.bytes.Bytes;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for {@link ECIESEncryptionEngine}. */
final class ECIESEncryptionEngineTest {

  @ParameterizedTest
  @ValueSource(ints = {0, 16, 17, 31, 32})
  void decryptRejectsInputsNotLongerThanMac(final int inputLength) {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final ECIESEncryptionEngine encryptionEngine =
        ECIESEncryptionEngine.forEncryption(nodeKey.getPublicKey());

    final ECIESEncryptionEngine decryptionEngine =
        ECIESEncryptionEngine.forDecryption(
            nodeKey, encryptionEngine.getEphPubKey(), encryptionEngine.getIv());

    assertThatThrownBy(() -> decryptionEngine.decrypt(Bytes.wrap(new byte[inputLength])))
        .isInstanceOf(InvalidCipherTextException.class)
        .hasMessage("Length of input must be greater than the MAC");
  }

  @Test
  void decryptAcceptsValidInputLongerThanMac() throws InvalidCipherTextException {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final ECIESEncryptionEngine encryptionEngine =
        ECIESEncryptionEngine.forEncryption(nodeKey.getPublicKey());

    final Bytes plaintext = Bytes.of(1);
    final Bytes encrypted = encryptionEngine.encrypt(plaintext);

    assertThat(encrypted.size()).isEqualTo(33);

    final ECIESEncryptionEngine decryptionEngine =
        ECIESEncryptionEngine.forDecryption(
            nodeKey, encryptionEngine.getEphPubKey(), encryptionEngine.getIv());

    assertThat(decryptionEngine.decrypt(encrypted)).isEqualTo(plaintext);
  }
}
