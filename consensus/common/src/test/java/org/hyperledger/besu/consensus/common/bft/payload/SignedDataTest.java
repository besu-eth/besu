/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.consensus.common.bft.payload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class SignedDataTest {

  private final Payload payload = new TestPayload();

  @Test
  void createRejectsASignatureThatDoesNotRecoverToAnAddress() {
    final SECPSignature signature = NonRecoverableSignature.create();
    // the signature really does not recover for this payload's own hash
    assertThat(Util.signatureToAddress(signature, payload.hashForSignature())).isNull();

    assertThatThrownBy(() -> SignedData.create(payload, signature))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not recover to an address");
  }

  @Test
  void createRecoversTheAuthorOfAValidSignature() {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final SignedData<Payload> signedData =
        SignedData.create(
            payload, nodeKey.sign(Bytes32.wrap(payload.hashForSignature().getBytes())));

    assertThat(signedData.getAuthor()).isEqualTo(Util.publicKeyToAddress(nodeKey.getPublicKey()));
  }

  private static class TestPayload implements Payload {
    private final ConsensusRoundIdentifier roundIdentifier = new ConsensusRoundIdentifier(1, 0);

    @Override
    public void writeTo(final RLPOutput rlpOutput) {
      rlpOutput.startList();
      rlpOutput.writeLongScalar(roundIdentifier.getSequenceNumber());
      rlpOutput.writeIntScalar(roundIdentifier.getRoundNumber());
      rlpOutput.endList();
    }

    @Override
    public int getMessageType() {
      return 0;
    }

    @Override
    public Hash hashForSignature() {
      return Hash.hash(encoded());
    }

    @Override
    public ConsensusRoundIdentifier getRoundIdentifier() {
      return roundIdentifier;
    }
  }
}
