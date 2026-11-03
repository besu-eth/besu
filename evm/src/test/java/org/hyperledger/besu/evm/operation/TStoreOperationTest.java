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
package org.hyperledger.besu.evm.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.frame.ExceptionalHaltReason.INSUFFICIENT_GAS;
import static org.hyperledger.besu.evm.testutils.TestEvmConfigurations.EVM_V1;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.testutils.ByteCodeBuilder;
import org.hyperledger.besu.evm.testutils.FakeBlockValues;
import org.hyperledger.besu.evm.testutils.TestCodeExecutor;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TStoreOperationTest {

  private static final GasCalculator gasCalculator = new CancunGasCalculator();

  private MessageFrame createMessageFrame(
      final Address address, final long initialGas, final long remainingGas) {
    final ToyWorld toyWorld = new ToyWorld();
    final WorldUpdater worldStateUpdater = toyWorld.updater();
    final BlockValues blockHeader = new FakeBlockValues(1337);
    final MessageFrame frame =
        new TestMessageFrameBuilder()
            .address(address)
            .worldUpdater(worldStateUpdater)
            .blockValues(blockHeader)
            .initialGas(initialGas)
            .build();
    worldStateUpdater.getOrCreate(address).setBalance(Wei.of(1));
    worldStateUpdater.commit();
    frame.setGasRemaining(remainingGas);

    return frame;
  }

  @Test
  void tstoreInsufficientGas() {
    long initialGas = 10_000L;
    long remainingGas = 99L; // TSTORE cost should be 100
    final TStoreOperation operation = new TStoreOperation(gasCalculator);
    final MessageFrame frame =
        createMessageFrame(Address.fromHexString("0x18675309"), initialGas, remainingGas);
    frame.pushStackItem(UInt256.ZERO);
    frame.pushStackItem(UInt256.fromHexString("0x01"));

    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(INSUFFICIENT_GAS);
  }

  @Test
  void tStoreSimpleTest() {
    long initialGas = 10_000L;
    long remainingGas = 10_000L;
    final TStoreOperation operation = new TStoreOperation(gasCalculator);
    final MessageFrame frame =
        createMessageFrame(Address.fromHexString("0x18675309"), initialGas, remainingGas);
    frame.pushStackItem(UInt256.ZERO);
    frame.pushStackItem(UInt256.fromHexString("0x01"));

    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
  }

  @Test
  void tLoadEmpty() {
    long initialGas = 10_000L;
    long remainingGas = 10_000L;
    final MessageFrame frame =
        createMessageFrame(Address.fromHexString("0x18675309"), initialGas, remainingGas);

    final TLoadOperation tload = new TLoadOperation(gasCalculator);
    frame.pushStackItem(UInt256.fromHexString("0x01"));
    final OperationResult tloadResult = tload.execute(frame, null);
    assertThat(tloadResult.getHaltReason()).isNull();
    var tloadValue = frame.popStackItem();
    assertThat(tloadValue).isEqualTo(Bytes32.ZERO);
  }

  @Test
  void tStoreTLoad() {
    long initialGas = 10_000L;
    long remainingGas = 10_000L;
    final TStoreOperation tstore = new TStoreOperation(gasCalculator);
    final MessageFrame frame =
        createMessageFrame(Address.fromHexString("0x18675309"), initialGas, remainingGas);
    frame.pushStackItem(UInt256.ONE);
    frame.pushStackItem(UInt256.fromHexString("0x01"));

    final OperationResult result = tstore.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();

    TLoadOperation tload = new TLoadOperation(gasCalculator);
    frame.pushStackItem(UInt256.fromHexString("0x01"));
    OperationResult tloadResult = tload.execute(frame, null);
    assertThat(tloadResult.getHaltReason()).isNull();
    UInt256 tloadValue = UInt256.fromBytes(frame.popStackItem());
    assertThat(tloadValue).isEqualTo(UInt256.ONE);

    // Loading from a different location returns default value
    frame.pushStackItem(UInt256.fromHexString("0x02"));
    tloadResult = tload.execute(frame, null);
    assertThat(tloadResult.getHaltReason()).isNull();
    tloadValue = UInt256.fromBytes(frame.popStackItem());
    assertThat(tloadValue).isEqualTo(UInt256.ZERO);
  }

  @Test
  void tStoreUpdate() {
    long initialGas = 10_000L;
    long remainingGas = 10_000L;
    final TStoreOperation tstore = new TStoreOperation(gasCalculator);
    final MessageFrame frame =
        createMessageFrame(Address.fromHexString("0x18675309"), initialGas, remainingGas);
    frame.pushStackItem(UInt256.ONE);
    frame.pushStackItem(UInt256.fromHexString("0x01"));

    OperationResult result = tstore.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();

    // Store 2 at position 1
    frame.pushStackItem(UInt256.fromHexString("0x02"));
    frame.pushStackItem(UInt256.fromHexString("0x01"));

    result = tstore.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();

    final TLoadOperation tload = new TLoadOperation(gasCalculator);
    frame.pushStackItem(UInt256.fromHexString("0x01"));
    final OperationResult tloadResult = tload.execute(frame, null);
    assertThat(tloadResult.getHaltReason()).isNull();
    UInt256 tloadValue = UInt256.fromBytes(frame.popStackItem());
    assertThat(tloadValue).isEqualTo(UInt256.fromHexString("0x02"));
  }

  // Zeroing out a transient storage slot does not result in gas refund
  @Test
  void noGasRefundFromTransientState() {
    long initialGas = 10_000L;
    long remainingGas = 10_000L;
    final TStoreOperation tstore = new TStoreOperation(gasCalculator);
    final MessageFrame frame =
        createMessageFrame(Address.fromHexString("0x18675309"), initialGas, remainingGas);
    frame.pushStackItem(UInt256.ONE);
    frame.pushStackItem(UInt256.fromHexString("0x01"));

    OperationResult result = tstore.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();

    // Reset value to 0
    frame.pushStackItem(UInt256.fromHexString("0x00"));
    frame.pushStackItem(UInt256.fromHexString("0x01"));

    result = tstore.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();

    assertThat(result.getGasCost()).isEqualTo(100L);
  }

  private static final UInt256 gasLimit = UInt256.valueOf(50_000);
  private static final Address contractAddress = Address.fromHexString("0x675910144");

  static Stream<Arguments> testParameters() {
    return Stream.of(
        // Tests specified in EIP-1153.
        Arguments.of(
            "Can TSTORE",
            null,
            new ByteCodeBuilder().tstore(1, 1),
            MessageFrame.State.COMPLETED_SUCCESS,
            0,
            1),
        Arguments.of(
            "Can tload uninitialized",
            null,
            new ByteCodeBuilder().tload(1).dataOnStackToMemory(0).returnValueAtMemory(32, 0),
            MessageFrame.State.COMPLETED_SUCCESS,
            0,
            1),
        Arguments.of(
            "Can tload after tstore",
            null,
            new ByteCodeBuilder()
                .tstore(1, 2)
                .tload(1)
                .dataOnStackToMemory(0)
                .returnValueAtMemory(32, 0),
            MessageFrame.State.COMPLETED_SUCCESS,
            2,
            1),
        Arguments.of(
            "Can tload after tstore from different location",
            null,
            new ByteCodeBuilder()
                .tstore(1, 2)
                .tload(2)
                .dataOnStackToMemory(0)
                .returnValueAtMemory(32, 0),
            MessageFrame.State.COMPLETED_SUCCESS,
            0,
            1),
        Arguments.of(
            "Contracts have separate transient storage",
            new ByteCodeBuilder().tload(1).dataOnStackToMemory(0).returnValueAtMemory(32, 0),
            new ByteCodeBuilder()
                .tstore(1, 2)
                .call(ByteCodeBuilder.Operation.CALL, contractAddress, gasLimit)
                .returnInnerCallResults(),
            MessageFrame.State.COMPLETED_SUCCESS,
            0,
            1),
        Arguments.of(
            "Reentrant calls access the same transient storage",
            new ByteCodeBuilder()
                // check if caller is self
                .callerIs(contractAddress)
                .conditionalJump(78)
                // non-reentrant, call self after tstore
                .tstore(1, 8)
                .call(ByteCodeBuilder.Operation.CALL, contractAddress, gasLimit)
                .returnInnerCallResults()
                // reentrant, TLOAD and return value
                .op(ByteCodeBuilder.Operation.JUMPDEST)
                .tload(1)
                .dataOnStackToMemory(0)
                .returnValueAtMemory(32, 0),
            new ByteCodeBuilder()
                .call(ByteCodeBuilder.Operation.CALL, contractAddress, gasLimit)
                .returnInnerCallResults(),
            MessageFrame.State.COMPLETED_SUCCESS,
            8,
            1),
        Arguments.of(
            "Successfully returned calls do not revert transient storage writes",
            new ByteCodeBuilder()
                // check if caller is self
                .callerIs(contractAddress)
                .conditionalJump(77)
                // non-reentrant, call self after tstore
                .tstore(1, 8)
                .call(ByteCodeBuilder.Operation.CALL, contractAddress, gasLimit)
                .tload(1)
                .dataOnStackToMemory(0)
                .returnValueAtMemory(32, 0)
                // reentrant, TLOAD and return value
                .op(ByteCodeBuilder.Operation.JUMPDEST)
                .tstore(1, 9),
            new ByteCodeBuilder()
                .call(ByteCodeBuilder.Operation.CALL, contractAddress, gasLimit)
                .returnInnerCallResults(),
            MessageFrame.State.COMPLETED_SUCCESS,
            9,
            1),
        Arguments.of(
            "Revert undoes the transient storage write from the failed call",
            new ByteCodeBuilder()
                // check if caller is self
                .callerIs(contractAddress)
                .conditionalJump(77)
                // non-reentrant, call self after tstore
                .tstore(1, 8)
                .call(ByteCodeBuilder.Operation.CALL, contractAddress, gasLimit)
                .tload(1)
                .dataOnStackToMemory(0)
                .returnValueAtMemory(32, 0)
                // reentrant, TLOAD and return value
                .op(ByteCodeBuilder.Operation.JUMPDEST)
                .tstore(1, 9)
                .op(ByteCodeBuilder.Operation.REVERT),
            new ByteCodeBuilder()
                .call(ByteCodeBuilder.Operation.CALL, contractAddress, gasLimit)
                .returnInnerCallResults(),
            MessageFrame.State.COMPLETED_SUCCESS,
            8,
            1),
        Arguments.of(
            "Revert undoes all the transient storage writes to the same key from the failed call",
            new ByteCodeBuilder()
                // check if caller is self
                .callerIs(contractAddress)
                .conditionalJump(77)
                // non-reentrant, call self after tstore
                .tstore(1, 8)
                .call(ByteCodeBuilder.Operation.CALL, contractAddress, gasLimit)
                .tload(1)
                .dataOnStackToMemory(0)
                .returnValueAtMemory(32, 0)
                // reentrant, TLOAD and return value
                .op(ByteCodeBuilder.Operation.JUMPDEST)
                .tstore(1, 9)
                .tstore(1, 10)
                .op(ByteCodeBuilder.Operation.REVERT),
            new ByteCodeBuilder()
                .call(ByteCodeBuilder.Operation.CALL, contractAddress, gasLimit)
                .returnInnerCallResults(),
            MessageFrame.State.COMPLETED_SUCCESS,
            8,
            1),
        Arguments.of(
            "Revert undoes transient storage writes from inner calls that successfully returned",
            new ByteCodeBuilder()
                // Check call depth
                .push(0)
                .op(ByteCodeBuilder.Operation.CALLDATALOAD)
                // Store input in mem and reload it to stack
                .dataOnStackToMemory(5)
                .push(5)
                .op(ByteCodeBuilder.Operation.MLOAD)

                // See if we're at call depth 1
                .push(1)
                .op(ByteCodeBuilder.Operation.EQ)
                .conditionalJump(84)

                // See if we're at call depth 2
                .push(5)
                .op(ByteCodeBuilder.Operation.MLOAD)
                .push(2)
                .op(ByteCodeBuilder.Operation.EQ)
                .conditionalJump(135)

                // Call depth = 0, call self after TSTORE 8
                .tstore(1, 8)

                // Recursive call with input
                // Depth++
                .push(5)
                .op(ByteCodeBuilder.Operation.MLOAD)
                .push(1)
                .op(ByteCodeBuilder.Operation.ADD)
                .callWithInput(ByteCodeBuilder.Operation.CALL, contractAddress, gasLimit)

                // TLOAD and return value
                .tload(1)
                .dataOnStackToMemory(0)
                .returnValueAtMemory(32, 0)

                // Call depth 1, TSTORE 9 but REVERT after recursion
                .op(ByteCodeBuilder.Operation.JUMPDEST)
                .tstore(1, 9)

                // Recursive call with input
                // Depth++
                .push(5)
                .op(ByteCodeBuilder.Operation.MLOAD)
                .push(1)
                .op(ByteCodeBuilder.Operation.ADD)
                .callWithInput(ByteCodeBuilder.Operation.CALL, contractAddress, gasLimit)
                .op(ByteCodeBuilder.Operation.REVERT)

                // Call depth 2, TSTORE 10 and complete
                .op(ByteCodeBuilder.Operation.JUMPDEST)
                .tstore(1, 10),
            new ByteCodeBuilder()
                // Call with input 0
                .callWithInput(
                    ByteCodeBuilder.Operation.CALL, contractAddress, gasLimit, UInt256.valueOf(0))
                .returnInnerCallResults(),
            MessageFrame.State.COMPLETED_SUCCESS,
            8,
            1),
        Arguments.of(
            "Transient storage cannot be manipulated in a static context",
            new ByteCodeBuilder()
                .tstore(1, 8)
                .tload(1)
                .dataOnStackToMemory(0)
                .returnValueAtMemory(32, 0),
            new ByteCodeBuilder()
                .call(ByteCodeBuilder.Operation.STATICCALL, contractAddress, gasLimit)
                .returnInnerCallResults(),
            MessageFrame.State.COMPLETED_FAILED,
            0,
            1),
        Arguments.of(
            "Transient storage cannot be manipulated in a static context when calling self",
            new ByteCodeBuilder()
                // Check if caller is self
                .callerIs(contractAddress)
                .conditionalJump(82)

                // Non-reentrant, call self after TSTORE 8
                .tstore(1, 8)
                .callWithInput(
                    ByteCodeBuilder.Operation.STATICCALL,
                    contractAddress,
                    gasLimit,
                    UInt256.valueOf(0))
                // Return the TLOAD value
                // Should be 8 if call fails, 9 if success
                .tload(1)
                .dataOnStackToMemory(0)
                .returnValueAtMemory(32, 0)

                // Reentrant, TSTORE 9
                .op(ByteCodeBuilder.Operation.JUMPDEST)
                .tstore(1, 9),
            new ByteCodeBuilder()
                .call(ByteCodeBuilder.Operation.CALL, contractAddress, gasLimit)
                .returnInnerCallResults(),
            MessageFrame.State.COMPLETED_SUCCESS,
            8,
            1),
        Arguments.of(
            "Transient storage cannot be manipulated in a nested static context",
            new ByteCodeBuilder()
                // Check call depth
                .push(0)
                .op(ByteCodeBuilder.Operation.CALLDATALOAD)
                // Store input in mem and reload it to stack
                .dataOnStackToMemory(5)
                .push(5)
                .op(ByteCodeBuilder.Operation.MLOAD)

                // See if we're at call depth 1
                .push(1)
                .op(ByteCodeBuilder.Operation.EQ)
                .conditionalJump(84)

                // See if we're at call depth 2
                .push(5)
                .op(ByteCodeBuilder.Operation.MLOAD)
                .push(2)
                .op(ByteCodeBuilder.Operation.EQ)
                .conditionalJump(140)

                // Call depth = 0, call self after TSTORE 8
                .tstore(1, 8)

                // Recursive call with input
                // Depth++
                .push(5)
                .op(ByteCodeBuilder.Operation.MLOAD)
                .push(1)
                .op(ByteCodeBuilder.Operation.ADD)
                .callWithInput(ByteCodeBuilder.Operation.STATICCALL, contractAddress, gasLimit)

                // TLOAD and return value
                .tload(1)
                .dataOnStackToMemory(0)
                .returnValueAtMemory(32, 0)

                // Call depth 1, TSTORE 9 but REVERT after recursion
                .op(ByteCodeBuilder.Operation.JUMPDEST) // 84

                // Recursive call with input
                // Depth++
                .push(5)
                .op(ByteCodeBuilder.Operation.MLOAD)
                .push(1)
                .op(ByteCodeBuilder.Operation.ADD)
                .callWithInput(ByteCodeBuilder.Operation.CALL, contractAddress, gasLimit)

                // TLOAD and return value
                .tload(1)
                .dataOnStackToMemory(0)
                .returnValueAtMemory(32, 0)

                // Call depth 2, TSTORE 10 and complete
                .op(ByteCodeBuilder.Operation.JUMPDEST) // 140
                .tstore(1, 10) // this call will fail
            ,
            new ByteCodeBuilder()
                // Call with input 0
                .callWithInput(
                    ByteCodeBuilder.Operation.CALL, contractAddress, gasLimit, UInt256.valueOf(0))
                .returnInnerCallResults(),
            MessageFrame.State.COMPLETED_SUCCESS,
            8,
            1),
        Arguments.of(
            "Delegatecall manipulates transient storage in the context of the current address",
            new ByteCodeBuilder().tstore(1, 8),
            new ByteCodeBuilder()
                .tstore(1, 7)
                .call(ByteCodeBuilder.Operation.DELEGATECALL, contractAddress, gasLimit)
                .tload(1)
                .dataOnStackToMemory(0)
                .returnValueAtMemory(32, 0),
            MessageFrame.State.COMPLETED_SUCCESS,
            8,
            1),
        Arguments.of(
            "Zeroing out a transient storage slot does not result in gas refund",
            null,
            new ByteCodeBuilder().tstore(1, 7).tstore(1, 0),
            MessageFrame.State.COMPLETED_SUCCESS,
            0,
            1),
        Arguments.of(
            "Transient storage can be accessed in a static context when calling self",
            new ByteCodeBuilder()
                // Check if caller is self
                .callerIs(contractAddress)
                .conditionalJump(78)

                // Non-reentrant, call self after TSTORE 8
                .tstore(1, 8)
                .call(ByteCodeBuilder.Operation.STATICCALL, contractAddress, gasLimit)
                .returnInnerCallResults()

                // Reentrant, TLOAD and return
                .op(ByteCodeBuilder.Operation.JUMPDEST)
                .tload(1)
                .dataOnStackToMemory(0)
                .returnValueAtMemory(32, 0),
            new ByteCodeBuilder()
                .call(ByteCodeBuilder.Operation.CALL, contractAddress, gasLimit)
                .returnInnerCallResults(),
            MessageFrame.State.COMPLETED_SUCCESS,
            8,
            1),
        Arguments.of(
            "Transient storage does not persist beyond a single transaction. The tstore should not have any impact on the second call",
            new ByteCodeBuilder()
                .tload(1)
                .tstore(1, 1)
                .dataOnStackToMemory(0)
                .returnValueAtMemory(32, 0),
            new ByteCodeBuilder()
                .call(ByteCodeBuilder.Operation.CALL, contractAddress, gasLimit)
                .returnInnerCallResults(),
            MessageFrame.State.COMPLETED_SUCCESS,
            0,
            2));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("testParameters")
  void transientStorageExecutionTest(
      final String ignoredName,
      final ByteCodeBuilder contractByteCodeBuilder,
      final ByteCodeBuilder byteCodeBuilder,
      final MessageFrame.State expectedResultState,
      final int expectedReturnValue,
      final int numberOfIterations) {

    TestCodeExecutor codeExecutor = new TestCodeExecutor(MainnetEVMs.cancun(EVM_V1));

    // Pre-deploy the contract if it's specified
    WorldUpdater worldUpdater =
        TestCodeExecutor.createInitialWorldState(
            account -> account.setStorageValue(UInt256.ZERO, UInt256.valueOf(0)));
    if (contractByteCodeBuilder != null) {
      WorldUpdater updater = worldUpdater.updater();
      TestCodeExecutor.deployContract(updater, contractAddress, contractByteCodeBuilder.toString());
      updater.commit();
    }
    for (int i = 0; i < numberOfIterations; i++) {
      final MessageFrame frame =
          codeExecutor.executeCode(
              byteCodeBuilder.toString(), gasLimit.toLong(), worldUpdater.updater());

      assertThat(frame.getState()).isEqualTo(expectedResultState);
      assertThat(frame.getGasRefund()).isZero();
      assertThat(UInt256.fromBytes(frame.getOutputData()).toInt()).isEqualTo(expectedReturnValue);
    }
  }
}
