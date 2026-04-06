/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.eth.messages.snap;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.StreamSupport;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class GetBlockAccessListsMessageTest {

  @Test
  public void roundTripWithRequestId() {
    final List<Hash> blockHashes =
        List.of(Hash.wrap(Bytes32.random()), Hash.wrap(Bytes32.random()));

    final GetBlockAccessListsMessage initialMessage =
        GetBlockAccessListsMessage.create(blockHashes);
    final MessageData wrapped = initialMessage.wrapMessageData(BigInteger.valueOf(7));
    final MessageData raw = new RawMessage(SnapV2.GET_BLOCK_ACCESS_LISTS, wrapped.getData());

    final GetBlockAccessListsMessage message = GetBlockAccessListsMessage.readFrom(raw);

    assertThat(StreamSupport.stream(message.blockHashes(true).spliterator(), false))
        .containsExactlyElementsOf(blockHashes);
  }

  @Test
  public void createWithEmptyHashes() {
    final MessageData initialMessage = GetBlockAccessListsMessage.create(List.of());
    final MessageData raw = new RawMessage(SnapV2.GET_BLOCK_ACCESS_LISTS, initialMessage.getData());
    final GetBlockAccessListsMessage message = GetBlockAccessListsMessage.readFrom(raw);

    assertThat(message.blockHashes(false)).isEmpty();
  }
}
