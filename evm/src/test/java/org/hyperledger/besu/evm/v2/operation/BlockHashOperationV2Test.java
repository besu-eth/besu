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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.UInt256;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.testutils.FakeBlockValues;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class BlockHashOperationV2Test {

  private static final long ENOUGH_GAS = 30_000_000L;
  private final FrontierGasCalculator gasCalculator = new FrontierGasCalculator();
  private final BlockHashOperationV2 operation = new BlockHashOperationV2(gasCalculator);

  @Test
  void shouldPushBlockHashWhenBlockIsWithinLookback() {
    final Hash blockHash = Hash.hash(Bytes.fromHexString("0x1293487297"));
    final MessageFrame frame =
        createFrame(100, 200, (__, block) -> block == 100 ? blockHash : Hash.ZERO, ENOUGH_GAS);

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isNull();
    assertThat(result.getGasCost()).isEqualTo(gasCalculator.getBlockHashOperationGasCost());
    assertThat(getV2StackItem(frame, 0))
        .isEqualTo(UInt256.fromBytesBE(blockHash.getBytes().toArrayUnsafe()));
    assertThat(frame.stackTopV2()).isEqualTo(1);
  }

  @Test
  void shouldPushZeroWhenBlockIsCurrentOrFuture() {
    final MessageFrame current = createFrame(200, 200, (__, ___) -> Hash.EMPTY, ENOUGH_GAS);
    final MessageFrame future = createFrame(201, 200, (__, ___) -> Hash.EMPTY, ENOUGH_GAS);

    operation.execute(current, null);
    operation.execute(future, null);

    assertThat(getV2StackItem(current, 0)).isEqualTo(UInt256.ZERO);
    assertThat(getV2StackItem(future, 0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void shouldPushZeroWhenBlockIsOutsideLookback() {
    final BlockHashLookup lookup =
        new BlockHashLookup() {
          @Override
          public Hash apply(final MessageFrame frame, final Long blockNumber) {
            return Hash.EMPTY;
          }

          @Override
          public long getLookback() {
            return 10;
          }
        };
    final MessageFrame frame = createFrame(89, 100, lookup, ENOUGH_GAS);

    operation.execute(frame, null);

    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void shouldPushZeroWhenArgumentDoesNotFitLong() {
    final MessageFrame frame =
        createFrame(
            Bytes32.fromHexString("0x010000000000000000"),
            200,
            (__, ___) -> Hash.EMPTY,
            ENOUGH_GAS);

    operation.execute(frame, null);

    assertThat(getV2StackItem(frame, 0)).isEqualTo(UInt256.ZERO);
  }

  @Test
  void shouldHaltOnInsufficientGasWithoutChangingStack() {
    final MessageFrame frame = createFrame(100, 200, (__, ___) -> Hash.EMPTY, 1);

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
    assertThat(getV2StackItem(frame, 0)).isEqualTo(new UInt256(0, 0, 0, 100));
  }

  @Test
  void shouldHaltOnStackUnderflow() {
    final MessageFrame frame = new TestMessageFrameBuilderV2().build();

    final OperationResult result = operation.execute(frame, null);

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  private MessageFrame createFrame(
      final long requestedBlock,
      final long currentBlock,
      final BlockHashLookup blockHashLookup,
      final long initialGas) {
    return createFrame(
        Bytes32.leftPad(Bytes.ofUnsignedLong(requestedBlock)),
        currentBlock,
        blockHashLookup,
        initialGas);
  }

  private MessageFrame createFrame(
      final Bytes32 requestedBlock,
      final long currentBlock,
      final BlockHashLookup blockHashLookup,
      final long initialGas) {
    return new TestMessageFrameBuilderV2()
        .blockHashLookup(blockHashLookup)
        .blockValues(new FakeBlockValues(currentBlock))
        .pushStackItem(requestedBlock)
        .initialGas(initialGas)
        .build();
  }
}
