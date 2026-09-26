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

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.qbft.core.messagedata.QbftV1;
import org.hyperledger.besu.consensus.qbft.core.payload.CommitPayload;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Util;

import java.math.BigInteger;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class CommitTest {

  @Test
  public void decodeSequenceReadsSequenceNumber() {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final CommitPayload payload =
        new CommitPayload(
            new ConsensusRoundIdentifier(77L, 0),
            Hash.ZERO,
            SignatureAlgorithmFactory.getInstance()
                .createSignature(BigInteger.ONE, BigInteger.ONE, (byte) 0));
    final SignedData<CommitPayload> signed =
        SignedData.create(
            payload, nodeKey.sign(Bytes32.wrap(payload.hashForSignature().getBytes())));
    assertThat(Commit.decodeSequence(new Commit(signed).encode())).isEqualTo(77L);
  }

  @Test
  public void canRoundTripACommitMessage() {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final Address addr = Util.publicKeyToAddress(nodeKey.getPublicKey());

    final CommitPayload commitPayload =
        new CommitPayload(
            new ConsensusRoundIdentifier(1, 1),
            Hash.ZERO,
            SignatureAlgorithmFactory.getInstance()
                .createSignature(BigInteger.ONE, BigInteger.ONE, (byte) 0));

    final SignedData<CommitPayload> signedCommitPayload =
        SignedData.create(
            commitPayload, nodeKey.sign(Bytes32.wrap(commitPayload.hashForSignature().getBytes())));

    final Commit commitMsg = new Commit(signedCommitPayload);

    final Commit decodedPrepare = Commit.decode(commitMsg.encode());

    assertThat(decodedPrepare.getMessageType()).isEqualTo(QbftV1.COMMIT);
    assertThat(decodedPrepare.getAuthor()).isEqualTo(addr);
    assertThat(decodedPrepare.getSignedPayload())
        .isEqualToComparingFieldByField(signedCommitPayload);
  }
}
