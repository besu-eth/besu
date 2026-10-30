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
package org.hyperledger.besu.evm;

import static org.hyperledger.besu.evm.frame.MessageFrame.Type.MESSAGE_CALL;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.operation.JumpOperation;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CodeTest {

  private static final int CURRENT_PC = 1;
  private EVM evm;

  @BeforeEach
  void startUp() {
    evm = MainnetEVMs.futureEips(EvmConfiguration.DEFAULT);
  }

  @Test
  void shouldReuseJumpDestMap() {
    final JumpOperation operation = new JumpOperation(evm.getGasCalculator());
    final Bytes jumpBytes = Bytes.fromHexString("0x6003565b00");
    final Code getsCached = new Code(jumpBytes);
    MessageFrame frame = createJumpFrame(getsCached);

    OperationResult result = operation.execute(frame, evm);
    assertNull(result.getHaltReason());
    final long[] jumpDestBitMask = getsCached.getJumpDestBitMask();
    assertNotNull(jumpDestBitMask);

    // do it again to prove we don't recalculate, and we hit the cache

    frame = createJumpFrame(getsCached);

    result = operation.execute(frame, evm);
    assertNull(result.getHaltReason());
    assertSame(jumpDestBitMask, getsCached.getJumpDestBitMask());
  }

  @NotNull
  private MessageFrame createJumpFrame(final Code getsCached) {
    final MessageFrame frame =
        MessageFrame.builder()
            .type(MESSAGE_CALL)
            .worldUpdater(mock(WorldUpdater.class))
            .initialGas(10_000L)
            .address(Address.ZERO)
            .originator(Address.ZERO)
            .contract(Address.ZERO)
            .gasPrice(Wei.ZERO)
            .inputData(Bytes.EMPTY)
            .sender(Address.ZERO)
            .value(Wei.ZERO)
            .apparentValue(Wei.ZERO)
            .code(getsCached)
            .blockValues(mock(BlockValues.class))
            .completer(f -> {})
            .miningBeneficiary(Address.ZERO)
            .blockHashLookup((__, ___) -> Hash.EMPTY)
            .build();

    frame.setPC(CURRENT_PC);
    frame.pushStackItem(UInt256.fromHexString("0x03"));
    return frame;
  }
}
