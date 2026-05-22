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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.qbft.core.QbftBlockTestFixture;
import org.hyperledger.besu.consensus.qbft.core.messagedata.QbftV1;
import org.hyperledger.besu.consensus.qbft.core.payload.PreparePayload;
import org.hyperledger.besu.consensus.qbft.core.payload.PreparedRoundMetadata;
import org.hyperledger.besu.consensus.qbft.core.payload.RoundChangePayload;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCodec;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RoundChangeTest {
  @Mock private QbftBlockCodec blockEncoder;

  private static final QbftBlock BLOCK = new QbftBlockTestFixture().build();

  @Test
  public void canRoundTripARoundChangeMessage() {
    // Stub the mock codec to round-trip a single byte for the block. Without writing real bytes
    // for the block the encoded RLP would have only 2 top-level items (mock writeTo is a no-op),
    // which decode interprets as a malformed message.
    final Bytes blockPlaceholder = Bytes.of(0xAB);
    when(blockEncoder.readFrom(any()))
        .thenAnswer(
            inv -> {
              inv.getArgument(0, RLPInput.class).readBytes();
              return BLOCK;
            });
    doAnswer(
            inv -> {
              inv.getArgument(1, RLPOutput.class).writeBytes(blockPlaceholder);
              return null;
            })
        .when(blockEncoder)
        .writeTo(any(QbftBlock.class), any(RLPOutput.class));

    final NodeKey nodeKey = NodeKeyUtils.generate();
    final Address addr = Util.publicKeyToAddress(nodeKey.getPublicKey());

    final RoundChangePayload payload =
        new RoundChangePayload(
            new ConsensusRoundIdentifier(1, 1),
            Optional.of(new PreparedRoundMetadata(BLOCK.getHash(), 0)));

    final SignedData<RoundChangePayload> signedRoundChangePayload =
        SignedData.create(
            payload, nodeKey.sign(Bytes32.wrap(payload.hashForSignature().getBytes())));

    final PreparePayload preparePayload =
        new PreparePayload(new ConsensusRoundIdentifier(1, 0), BLOCK.getHash());
    final SignedData<PreparePayload> signedPreparePayload =
        SignedData.create(
            preparePayload,
            nodeKey.sign(Bytes32.wrap(preparePayload.hashForSignature().getBytes())));

    final RoundChange roundChange =
        new RoundChange(
            signedRoundChangePayload,
            Optional.of(BLOCK),
            Optional.empty(),
            blockEncoder,
            List.of(signedPreparePayload));

    final RoundChange decodedRoundChange = RoundChange.decode(roundChange.encode(), blockEncoder);

    assertThat(decodedRoundChange.getMessageType()).isEqualTo(QbftV1.ROUND_CHANGE);
    assertThat(decodedRoundChange.getAuthor()).isEqualTo(addr);
    assertThat(decodedRoundChange.getSignedPayload())
        .isEqualToComparingFieldByField(signedRoundChangePayload);
    assertThat(decodedRoundChange.getProposedBlock()).isNotEmpty();
    assertThat(decodedRoundChange.getProposedBlock().get()).isEqualToComparingFieldByField(BLOCK);
    assertThat(decodedRoundChange.getPrepares()).hasSize(1);
    assertThat(decodedRoundChange.getPrepares().getFirst())
        .isEqualToComparingFieldByField(signedPreparePayload);
  }

  @Test
  public void canRoundTripEmptyPreparedRoundAndPreparedList() {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final Address addr = Util.publicKeyToAddress(nodeKey.getPublicKey());

    final RoundChangePayload payload =
        new RoundChangePayload(new ConsensusRoundIdentifier(1, 1), Optional.empty());

    final SignedData<RoundChangePayload> signedRoundChangePayload =
        SignedData.create(
            payload, nodeKey.sign(Bytes32.wrap(payload.hashForSignature().getBytes())));

    final RoundChange roundChange =
        new RoundChange(
            signedRoundChangePayload,
            Optional.empty(),
            Optional.empty(),
            blockEncoder,
            Collections.emptyList());

    final RoundChange decodedRoundChange = RoundChange.decode(roundChange.encode(), blockEncoder);

    assertThat(decodedRoundChange.getMessageType()).isEqualTo(QbftV1.ROUND_CHANGE);
    assertThat(decodedRoundChange.getAuthor()).isEqualTo(addr);
    assertThat(decodedRoundChange.getSignedPayload())
        .isEqualToComparingFieldByField(signedRoundChangePayload);
    assertThat(decodedRoundChange.getProposedBlock()).isEmpty();
    assertThat(decodedRoundChange.getPrepares()).isEmpty();
  }

  @Test
  public void canDecodeRoundChangeFromLegacyNodeWithoutBlockAccessList() {
    // Simulate a pre-26.1.0 validator that encodes RoundChange WITHOUT the blockAccessList field.
    // Legacy: [SignedPayload, EmptyList, [Prepares]]           (3 items)
    // Current: [SignedPayload, EmptyList, BAL/null, [Prepares]] (4 items)
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final Address addr = Util.publicKeyToAddress(nodeKey.getPublicKey());

    final RoundChangePayload payload =
        new RoundChangePayload(new ConsensusRoundIdentifier(1, 1), Optional.empty());

    final SignedData<RoundChangePayload> signedRoundChangePayload =
        SignedData.create(
            payload, nodeKey.sign(Bytes32.wrap(payload.hashForSignature().getBytes())));

    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    rlpOut.startList();
    signedRoundChangePayload.writeTo(rlpOut);
    rlpOut.writeEmptyList();
    rlpOut.writeList(Collections.<SignedData<PreparePayload>>emptyList(), SignedData::writeTo);
    rlpOut.endList();
    final Bytes legacyEncoded = rlpOut.encoded();

    final RoundChange decodedRoundChange = RoundChange.decode(legacyEncoded, blockEncoder);

    assertThat(decodedRoundChange.getMessageType()).isEqualTo(QbftV1.ROUND_CHANGE);
    assertThat(decodedRoundChange.getAuthor()).isEqualTo(addr);
    assertThat(decodedRoundChange.getProposedBlock()).isEmpty();
    assertThat(decodedRoundChange.getBlockAccessList()).isEmpty();
    assertThat(decodedRoundChange.getPrepares()).isEmpty();
  }

  @Test
  public void canDecodeLegacyRoundChangeWithNonEmptyPrepares() {
    // Minimal reproduction of the user-reported stack trace: a pre-26.1.0 validator sends a
    // legacy 3-item RoundChange where the Prepares list is non-empty. Without the item-count
    // check in decode(), readBlockAccessList misreads the Prepares list as a BlockAccessList
    // and the subsequent readList throws "Cannot enter a lists, input is fully consumed".
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final Address addr = Util.publicKeyToAddress(nodeKey.getPublicKey());

    final RoundChangePayload payload =
        new RoundChangePayload(
            new ConsensusRoundIdentifier(1, 1),
            Optional.of(new PreparedRoundMetadata(BLOCK.getHash(), 0)));

    final SignedData<RoundChangePayload> signedRoundChangePayload =
        SignedData.create(
            payload, nodeKey.sign(Bytes32.wrap(payload.hashForSignature().getBytes())));

    final PreparePayload preparePayload =
        new PreparePayload(new ConsensusRoundIdentifier(1, 0), BLOCK.getHash());
    final SignedData<PreparePayload> signedPreparePayload =
        SignedData.create(
            preparePayload,
            nodeKey.sign(Bytes32.wrap(preparePayload.hashForSignature().getBytes())));

    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    rlpOut.startList();
    signedRoundChangePayload.writeTo(rlpOut);
    rlpOut.writeEmptyList(); // block absent (legacy 3-item layout)
    rlpOut.writeList(List.of(signedPreparePayload), SignedData::writeTo);
    rlpOut.endList();
    final Bytes legacyEncoded = rlpOut.encoded();

    final RoundChange decodedRoundChange = RoundChange.decode(legacyEncoded, blockEncoder);

    assertThat(decodedRoundChange.getMessageType()).isEqualTo(QbftV1.ROUND_CHANGE);
    assertThat(decodedRoundChange.getAuthor()).isEqualTo(addr);
    assertThat(decodedRoundChange.getProposedBlock()).isEmpty();
    assertThat(decodedRoundChange.getBlockAccessList()).isEmpty();
    assertThat(decodedRoundChange.getPrepares()).hasSize(1);
    assertThat(decodedRoundChange.getPrepares().getFirst())
        .isEqualToComparingFieldByField(signedPreparePayload);
  }

  @Test
  public void defaultEncodingEmitsCurrentFourItemWireFormat() {
    // Default useLegacyEncoding=false: RoundChange.encode() emits 4 top-level items
    // [SignedPayload, Block-or-emptyList, BAL-or-null, Prepares] - required for interop with
    // Besu 26.1.0 - 26.5.0 peers whose decoder expects the BAL slot.
    final NodeKey nodeKey = NodeKeyUtils.generate();

    final RoundChangePayload payload =
        new RoundChangePayload(new ConsensusRoundIdentifier(1, 1), Optional.empty());
    final SignedData<RoundChangePayload> signedRoundChangePayload =
        SignedData.create(
            payload, nodeKey.sign(Bytes32.wrap(payload.hashForSignature().getBytes())));

    final RoundChange roundChange =
        new RoundChange(
            signedRoundChangePayload,
            Optional.empty(),
            Optional.empty(),
            blockEncoder,
            Collections.emptyList());

    final org.hyperledger.besu.ethereum.rlp.RLPInput rlpIn =
        org.hyperledger.besu.ethereum.rlp.RLP.input(roundChange.encode());
    assertThat(rlpIn.enterList()).isEqualTo(4);
  }

  @Test
  public void legacyEncodingOmitsBlockAccessListSlot() {
    // When useLegacyEncoding=true, RoundChange.encode() emits 3 top-level items - the BAL slot
    // is omitted when absent. Required for interop with Besu 25.x peers during rolling upgrade.
    final NodeKey nodeKey = NodeKeyUtils.generate();

    final RoundChangePayload payload =
        new RoundChangePayload(new ConsensusRoundIdentifier(1, 1), Optional.empty());
    final SignedData<RoundChangePayload> signedRoundChangePayload =
        SignedData.create(
            payload, nodeKey.sign(Bytes32.wrap(payload.hashForSignature().getBytes())));

    final RoundChange roundChange =
        RoundChange.withLegacyEncoding(
            signedRoundChangePayload,
            Optional.empty(),
            Optional.empty(),
            blockEncoder,
            Collections.emptyList());

    final org.hyperledger.besu.ethereum.rlp.RLPInput rlpIn =
        org.hyperledger.besu.ethereum.rlp.RLP.input(roundChange.encode());
    assertThat(rlpIn.enterList()).isEqualTo(3);
  }
}
