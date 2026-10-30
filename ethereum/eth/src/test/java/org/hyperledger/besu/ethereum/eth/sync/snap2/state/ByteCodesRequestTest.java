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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.snap.ByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SnapMessages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.math.BigInteger;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class ByteCodesRequestTest {

  private static final Bytes FIRST = Bytes.of(1, 2, 3);
  private static final Bytes SECOND = Bytes.of(4, 5);
  private static final Bytes THIRD = Bytes.of(6);
  private static final List<Bytes32> HASHES = List.of(hash(FIRST), hash(SECOND), hash(THIRD));

  private final ByteCodesRequest request = new ByteCodesRequest(HASHES, 4096);
  private final EthPeer peer = mock(EthPeer.class);

  @Test
  void asksForTheCodesByHash() {
    assertThat(request.message(peer).getData())
        .isEqualTo(SnapMessages.getByteCodes(HASHES, 4096).getData());
  }

  @Test
  void keepsTheCodesServedInRequestOrderWithGaps() {
    assertThat(request.decode(response(FIRST, THIRD), peer))
        .containsExactly(entry(hash(FIRST), FIRST), entry(hash(THIRD), THIRD));
  }

  @Test
  void rejectsACodeNotRequested() {
    assertThatThrownBy(() -> request.decode(response(FIRST, Bytes.of(9)), peer))
        .isInstanceOf(SyncRequest.InvalidResponseException.class);
  }

  @Test
  void rejectsCodesOutOfOrder() {
    assertThatThrownBy(() -> request.decode(response(THIRD, FIRST), peer))
        .isInstanceOf(SyncRequest.InvalidResponseException.class);
  }

  @Test
  void treatsAnAnswerWithoutCodesAsEmpty() {
    assertThatThrownBy(() -> request.decode(response(), peer))
        .isInstanceOf(SyncRequest.EmptyResponseException.class);
  }

  private static MessageData response(final Bytes... codes) {
    return ByteCodesMessage.create(List.of(codes)).wrapMessageData(BigInteger.ONE);
  }

  private static Bytes32 hash(final Bytes code) {
    return Bytes32.wrap(Hash.hash(code).getBytes());
  }
}
