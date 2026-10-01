/*
 * Copyright 2020 ConsenSys AG.
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
package org.hyperledger.besu.consensus.qbft.core.messagewrappers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.qbft.core.messagedata.QbftV1;
import org.hyperledger.besu.consensus.qbft.core.payload.PreparePayload;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class PrepareTest {

  @Test
  public void decodeSequenceReadsSequenceNumber() {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final PreparePayload payload =
        new PreparePayload(new ConsensusRoundIdentifier(42L, 0), Hash.ZERO);
    final SignedData<PreparePayload> signed =
        SignedData.create(
            payload, nodeKey.sign(Bytes32.wrap(payload.hashForSignature().getBytes())));
    assertThat(Prepare.decodeSequence(new Prepare(signed).encode())).isEqualTo(42L);
  }

  @Test
  public void decodeSequenceThrowsRlpExceptionForMalformedData() {
    assertThatThrownBy(() -> Prepare.decodeSequence(Bytes.of(0x01, 0x02, 0x03)))
        .isInstanceOf(RLPException.class);
  }

  @Test
  public void canRoundTripAPrepareMessage() {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final Address addr = Util.publicKeyToAddress(nodeKey.getPublicKey());

    final PreparePayload preparePayload =
        new PreparePayload(new ConsensusRoundIdentifier(1, 1), Hash.ZERO);

    final SignedData<PreparePayload> signedPreparePayload =
        SignedData.create(
            preparePayload,
            nodeKey.sign(Bytes32.wrap(preparePayload.hashForSignature().getBytes())));

    final Prepare prepareMsg = new Prepare(signedPreparePayload);

    final Prepare decodedPrepare = Prepare.decode(prepareMsg.encode());

    assertThat(decodedPrepare.getMessageType()).isEqualTo(QbftV1.PREPARE);
    assertThat(decodedPrepare.getAuthor()).isEqualTo(addr);
    assertThat(decodedPrepare.getSignedPayload())
        .isEqualToComparingFieldByField(signedPreparePayload);
  }
}
