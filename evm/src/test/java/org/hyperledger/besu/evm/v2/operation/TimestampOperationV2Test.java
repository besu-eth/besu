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
import static org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2.getV2StackItem;

import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.BerlinGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.testutils.FakeBlockValues;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.junit.jupiter.api.Test;

class TimestampOperationV2Test extends NullaryOperationV2Test {

  private final GasCalculator gasCalculator = new BerlinGasCalculator();

  TimestampOperationV2Test() {
    super(new TimestampOperationV2(new BerlinGasCalculator()));
  }

  @Test
  void shouldPushTimestampToStack() {
    final MessageFrame frame = createFrame(1_753_802_400L);

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(getV2StackItem(frame, 0)).isEqualTo(new UInt256(0, 0, 0, 1_753_802_400L));
  }

  @Test
  void shouldPushUnsignedLongTimestamp() {
    final MessageFrame frame = createFrame(Long.MIN_VALUE);

    operation.execute(frame, null);

    assertThat(getV2StackItem(frame, 0)).isEqualTo(new UInt256(0, 0, 0, Long.MIN_VALUE));
  }

  @Test
  void shouldReturnCorrectGasCost() {
    final OperationResult result = operation.execute(createFrame(0), null);

    assertThat(result.getGasCost()).isEqualTo(gasCalculator.getBaseTierGasCost());
  }

  private MessageFrame createFrame(final long timestamp) {
    final FakeBlockValues blockValues =
        new FakeBlockValues(1337) {
          @Override
          public long getTimestamp() {
            return timestamp;
          }
        };
    return new TestMessageFrameBuilderV2().blockValues(blockValues).build();
  }
}
