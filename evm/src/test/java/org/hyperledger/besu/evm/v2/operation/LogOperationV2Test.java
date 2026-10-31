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
package org.hyperledger.besu.evm.v2.operation;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.LogTopic;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class LogOperationV2Test {

  private static final Address ADDRESS = Address.fromHexString("0xc0de");
  private static final Bytes MEMORY = Bytes.fromHexString("0x0102030405060708090a");
  private static final Bytes32 TOPIC_1 =
      Bytes32.fromHexString("0x0123456789abcdeffedcba98765432100f1e2d3c4b5a69788796a5b4c3d2e1f0");
  private static final Bytes32 TOPIC_2 = Bytes32.fromHexString("0x02");

  @Test
  void logsTheDataWithTheTopicsInStackOrder() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .address(ADDRESS)
            .memory(MEMORY)
            .pushStackItem(TOPIC_2)
            .pushStackItem(TOPIC_1)
            .pushStackItem(Bytes32.fromHexString("0x03"))
            .pushStackItem(Bytes32.fromHexString("0x01"))
            .build();

    final Operation.OperationResult result =
        new LogOperationV2(2, new FrontierGasCalculator()).execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(frame.stackTopV2()).isZero();
    assertThat(frame.getLogs())
        .containsExactly(
            new Log(
                ADDRESS,
                Bytes.fromHexString("0x020304"),
                List.of(LogTopic.create(TOPIC_1), LogTopic.create(TOPIC_2))));
    // 375 for the log, 375 for each topic and 8 for each byte of data
    assertThat(result.getGasCost()).isEqualTo(375 + 2 * 375 + 3 * 8);
  }

  @Test
  void logsWithoutTopics() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .address(ADDRESS)
            .pushStackItem(Bytes32.fromHexString("0x00"))
            .pushStackItem(Bytes32.fromHexString("0x00"))
            .build();

    new LogOperationV2(0, new FrontierGasCalculator()).execute(frame, null);

    assertThat(frame.getLogs()).containsExactly(new Log(ADDRESS, Bytes.EMPTY, List.of()));
  }

  @Test
  void haltsInAStaticFrame() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .isStatic(true)
            .pushStackItem(Bytes32.fromHexString("0x00"))
            .pushStackItem(Bytes32.fromHexString("0x00"))
            .build();

    final Operation.OperationResult result =
        new LogOperationV2(0, new FrontierGasCalculator()).execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    assertThat(frame.getLogs()).isEmpty();
  }

  @Test
  void haltsWithoutTheTopics() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .pushStackItem(TOPIC_1)
            .pushStackItem(Bytes32.fromHexString("0x00"))
            .pushStackItem(Bytes32.fromHexString("0x00"))
            .build();

    final Operation.OperationResult result =
        new LogOperationV2(2, new FrontierGasCalculator()).execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    assertThat(frame.getLogs()).isEmpty();
  }

  @Test
  void haltsOnALengthBeyondAnyGas() {
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .initialGas(1_000_000)
            .pushStackItem(Bytes32.fromHexString("0x0100000000000000"))
            .pushStackItem(Bytes32.fromHexString("0x00"))
            .build();

    final Operation.OperationResult result =
        new LogOperationV2(0, new FrontierGasCalculator()).execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
  }
}
