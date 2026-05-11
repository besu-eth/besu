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
package org.hyperledger.besu.ethereum.eth.messages;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;

import java.util.Arrays;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class GetPooledTransactionsMessageTest {

  @Test
  public void roundTripGetPooledTransactionsMessage() {
    List<Hash> hashes = Arrays.asList(Hash.wrap(Bytes32.random()));
    final GetPooledTransactionsMessage msg = GetPooledTransactionsMessage.create(hashes);
    assertThat(msg.getCode()).isEqualTo(EthProtocolMessages.GET_POOLED_TRANSACTIONS);
    assertThat(msg.pooledTransactions()).containsExactlyElementsOf(hashes);
  }

  @Test
  public void readFromMessageWithWrongCodeThrows() {
    final RawMessage rawMsg = new RawMessage(EthProtocolMessages.BLOCK_HEADERS, Bytes.of(0));

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> GetPooledTransactionsMessage.readFrom(rawMsg));
  }
}
