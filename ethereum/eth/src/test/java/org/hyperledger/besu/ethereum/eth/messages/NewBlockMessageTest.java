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
import static wtf.metio.storageunits.model.StorageUnits.mebibyte;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class NewBlockMessageTest {
  private static final ProtocolSchedule protocolSchedule = ProtocolScheduleFixture.TESTING_NETWORK;
  private static final int maxMessageSize = mebibyte(10).inByte().intValueExact();

  @Test
  public void roundTripNewBlockMessage() {
    final Difficulty totalDifficulty = Difficulty.of(98765);
    final BlockDataGenerator blockGenerator = new BlockDataGenerator();
    final Block blockForInsertion = blockGenerator.block();

    final NewBlockMessage msg =
        NewBlockMessage.create(blockForInsertion, totalDifficulty, maxMessageSize);
    assertThat(msg.getCode()).isEqualTo(EthProtocolMessages.NEW_BLOCK);
    assertThat(msg.totalDifficulty(protocolSchedule)).isEqualTo(totalDifficulty);
    final Block extractedBlock = msg.block(protocolSchedule);
    assertThat(extractedBlock).isEqualTo(blockForInsertion);
  }

  @Test
  public void rawMessageUpCastsToANewBlockMessage() {
    final Difficulty totalDifficulty = Difficulty.of(12345);
    final BlockDataGenerator blockGenerator = new BlockDataGenerator();
    final Block blockForInsertion = blockGenerator.block();

    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    blockForInsertion.writeTo(tmp);
    tmp.writeUInt256Scalar(totalDifficulty);
    tmp.endList();

    final RawMessage rawMsg = new RawMessage(EthProtocolMessages.NEW_BLOCK, tmp.encoded());

    final NewBlockMessage newBlockMsg = NewBlockMessage.readFrom(rawMsg);

    assertThat(newBlockMsg.getCode()).isEqualTo(EthProtocolMessages.NEW_BLOCK);
    assertThat(newBlockMsg.totalDifficulty(protocolSchedule)).isEqualTo(totalDifficulty);
    final Block extractedBlock = newBlockMsg.block(protocolSchedule);
    assertThat(extractedBlock).isEqualTo(blockForInsertion);
  }

  @Test
  public void readFromMessageWithWrongCodeThrows() {
    final RawMessage rawMsg = new RawMessage(EthProtocolMessages.BLOCK_HEADERS, Bytes.of(0));

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> NewBlockMessage.readFrom(rawMsg));
  }

  @Test
  public void createBlockMessageLargerThanLimitThrows() {
    final Difficulty totalDifficulty = Difficulty.of(98765);
    final BlockDataGenerator blockGenerator = new BlockDataGenerator();
    final Block newBlock = blockGenerator.block();

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> NewBlockMessage.create(newBlock, totalDifficulty, 1));
  }
}
