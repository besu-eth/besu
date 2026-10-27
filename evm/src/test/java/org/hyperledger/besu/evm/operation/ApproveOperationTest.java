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
package org.hyperledger.besu.evm.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.collections.undo.UndoSet;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.FrameTransactionContext.FrameInfo;
import org.hyperledger.besu.evm.frame.KeyedNonces;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.frame.TxValues;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.StateGasCostCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.operation.ApproveOperation.ApprovalResult;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.processor.MessageCallProcessor;
import org.hyperledger.besu.evm.toy.ToyBlockValues;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ApproveOperationTest {

  private static final Address SENDER = Address.fromHexString("0x5e4d");
  private static final Address PAYMASTER = Address.fromHexString("0x9a7e");
  private static final Address OTHER = Address.fromHexString("0x07e4");
  private static final Wei MAX_COST = Wei.of(1_000);

  private static final int EXECUTION = FrameTransactionContext.APPROVE_EXECUTION;
  private static final int PAYMENT = FrameTransactionContext.APPROVE_PAYMENT;
  private static final int BOTH = FrameTransactionContext.APPROVE_SCOPE_MASK;

  // PUSH1 3 (scope) PUSH1 0 (length) PUSH1 0 (offset) APPROVE
  private static final Bytes APPROVE_BOTH = Bytes.fromHexString("0x600360006000aa");

  private static final GasCalculator GAS_CALCULATOR = new AmsterdamGasCalculator();
  private static final StateGasCostCalculator STATE_GAS = GAS_CALCULATOR.stateGasCostCalculator();

  private static final EvmConfiguration V2 =
      new EvmConfiguration(
          EvmConfiguration.DEFAULT.jumpDestCacheWeightKB(),
          EvmConfiguration.DEFAULT.worldUpdaterMode(),
          EvmConfiguration.DEFAULT.enableOptimizedOpcodes(),
          true);

  private final ApproveOperation operation = new ApproveOperation(GAS_CALCULATOR);

  private ToyWorld world;

  @BeforeEach
  void setUp() {
    world = new ToyWorld();
  }

  // ---------------------------------------------------------------------------------------------
  // attemptApproval
  // ---------------------------------------------------------------------------------------------

  @Test
  void senderApprovesExecution() {
    account(SENDER, 3, Wei.of(5_000));
    final FrameTransactionContext context = legacyContext(frame(BOTH, SENDER));

    assertThat(approve(context, EXECUTION, 0L)).isEqualTo(ApprovalResult.SUCCESS);

    assertThat(context.senderApproved()).isTrue();
    assertThat(context.payer()).isNull();
    assertThat(world.get(SENDER).getNonce()).isEqualTo(3L);
    assertThat(world.get(SENDER).getBalance()).isEqualTo(Wei.of(5_000));
  }

  @Test
  void onlyTheSenderApprovesExecution() {
    final FrameTransactionContext context = legacyContext(frame(BOTH, PAYMASTER));

    assertThat(approve(context, EXECUTION, 0L)).isEqualTo(ApprovalResult.REVERT);
    assertThat(context.senderApproved()).isFalse();
  }

  @Test
  void executionIsApprovedOnlyOnce() {
    final FrameTransactionContext context = legacyContext(frame(BOTH, SENDER));

    assertThat(approve(context, EXECUTION, 0L)).isEqualTo(ApprovalResult.SUCCESS);
    assertThat(approve(context, EXECUTION, 0L)).isEqualTo(ApprovalResult.REVERT);
  }

  @Test
  void paymentBeforeExecutionReverts() {
    account(PAYMASTER, 0, Wei.of(5_000));
    final FrameTransactionContext context = legacyContext(frame(PAYMENT, PAYMASTER));

    assertThat(approve(context, PAYMENT, 0L)).isEqualTo(ApprovalResult.REVERT);

    assertThat(context.payer()).isNull();
    assertThat(world.get(PAYMASTER).getBalance()).isEqualTo(Wei.of(5_000));
  }

  @Test
  void scopeMustBeNonEmptyAndAllowedByTheFrame() {
    account(SENDER, 0, Wei.of(5_000));
    final FrameTransactionContext context = legacyContext(frame(EXECUTION, SENDER));

    assertThat(approve(context, 0, 0L)).isEqualTo(ApprovalResult.REVERT);
    assertThat(approve(context, BOTH, 0L)).isEqualTo(ApprovalResult.REVERT);
    assertThat(approve(context, PAYMENT, 0L)).isEqualTo(ApprovalResult.REVERT);

    assertThat(context.senderApproved()).isFalse();
    assertThat(context.payer()).isNull();
  }

  @Test
  void senderApprovingBothScopesPaysAndConsumesTheLegacyNonce() {
    account(SENDER, 3, Wei.of(5_000));
    final FrameTransactionContext context = legacyContext(frame(BOTH, SENDER));

    final MessageFrame messageFrame = messageFrame(context, SENDER, 0L);
    assertThat(ApproveOperation.attemptApproval(messageFrame, context, BOTH, STATE_GAS))
        .isEqualTo(ApprovalResult.SUCCESS);

    assertThat(context.senderApproved()).isTrue();
    assertThat(context.payer()).isEqualTo(SENDER);
    assertThat(world.get(SENDER).getBalance()).isEqualTo(Wei.of(4_000));
    assertThat(world.get(SENDER).getNonce()).isEqualTo(4L);
    // an existing account is not charged state gas for its nonce
    assertThat(messageFrame.getStateGasReservoir()).isZero();
    assertThat(messageFrame.getStateGasUsed()).isZero();
  }

  @Test
  void paymasterPaysOnceTheSenderApprovedExecution() {
    account(SENDER, 3, Wei.of(5_000));
    account(PAYMASTER, 0, Wei.of(2_000));
    final FrameTransactionContext context =
        legacyContext(frame(EXECUTION, SENDER), frame(PAYMENT, PAYMASTER));
    assertThat(approve(context, EXECUTION, 0L)).isEqualTo(ApprovalResult.SUCCESS);

    context.setCurrentFrameIndex(1);
    assertThat(approve(context, PAYMENT, 0L)).isEqualTo(ApprovalResult.SUCCESS);

    assertThat(context.payer()).isEqualTo(PAYMASTER);
    assertThat(world.get(PAYMASTER).getBalance()).isEqualTo(Wei.of(1_000));
    assertThat(world.get(SENDER).getBalance()).isEqualTo(Wei.of(5_000));
    assertThat(world.get(SENDER).getNonce()).isEqualTo(4L);
  }

  @Test
  void paymentIsApprovedOnlyOnce() {
    account(SENDER, 3, Wei.of(5_000));
    account(PAYMASTER, 0, Wei.of(5_000));
    final FrameTransactionContext context =
        legacyContext(frame(BOTH, SENDER), frame(PAYMENT, PAYMASTER));
    assertThat(approve(context, BOTH, 0L)).isEqualTo(ApprovalResult.SUCCESS);

    context.setCurrentFrameIndex(1);
    assertThat(approve(context, PAYMENT, 0L)).isEqualTo(ApprovalResult.REVERT);

    assertThat(context.payer()).isEqualTo(SENDER);
    assertThat(world.get(PAYMASTER).getBalance()).isEqualTo(Wei.of(5_000));
    assertThat(world.get(SENDER).getNonce()).isEqualTo(4L);
  }

  @Test
  void payerBelowTheMaxCostReverts() {
    account(SENDER, 3, Wei.of(5_000));
    account(PAYMASTER, 0, MAX_COST.subtract(Wei.ONE));
    final FrameTransactionContext context =
        legacyContext(frame(EXECUTION, SENDER), frame(PAYMENT, PAYMASTER));
    assertThat(approve(context, EXECUTION, 0L)).isEqualTo(ApprovalResult.SUCCESS);

    context.setCurrentFrameIndex(1);
    assertThat(approve(context, PAYMENT, 0L)).isEqualTo(ApprovalResult.REVERT);

    assertThat(context.payer()).isNull();
    assertThat(world.get(PAYMASTER).getBalance()).isEqualTo(MAX_COST.subtract(Wei.ONE));
    assertThat(world.get(SENDER).getNonce()).isEqualTo(3L);
  }

  @Test
  void legacyNonceOfAnEmptySenderIsChargedNewAccountStateGas() {
    account(PAYMASTER, 0, Wei.of(5_000));
    final FrameTransactionContext context =
        legacyContext(frame(EXECUTION, SENDER), frame(PAYMENT, PAYMASTER));
    assertThat(approve(context, EXECUTION, 0L)).isEqualTo(ApprovalResult.SUCCESS);
    context.setCurrentFrameIndex(1);

    final MessageFrame messageFrame =
        messageFrame(context, PAYMASTER, STATE_GAS.newAccountStateGas() + 5);
    assertThat(ApproveOperation.attemptApproval(messageFrame, context, PAYMENT, STATE_GAS))
        .isEqualTo(ApprovalResult.SUCCESS);

    assertThat(messageFrame.getStateGasReservoir()).isEqualTo(5L);
    assertThat(messageFrame.getStateGasUsed()).isEqualTo(STATE_GAS.newAccountStateGas());
    assertThat(world.get(SENDER).getNonce()).isEqualTo(1L);
    assertThat(context.payer()).isEqualTo(PAYMASTER);
  }

  @Test
  void unaffordableNonceStateGasLeavesEverythingUntouched() {
    account(PAYMASTER, 0, Wei.of(5_000));
    final FrameTransactionContext context =
        legacyContext(frame(EXECUTION, SENDER), frame(PAYMENT, PAYMASTER));
    assertThat(approve(context, EXECUTION, 0L)).isEqualTo(ApprovalResult.SUCCESS);
    context.setCurrentFrameIndex(1);

    final MessageFrame messageFrame =
        messageFrame(context, PAYMASTER, STATE_GAS.newAccountStateGas() - 1);
    assertThat(ApproveOperation.attemptApproval(messageFrame, context, PAYMENT, STATE_GAS))
        .isEqualTo(ApprovalResult.OUT_OF_STATE_GAS);

    assertThat(messageFrame.getStateGasReservoir()).isEqualTo(STATE_GAS.newAccountStateGas() - 1);
    assertThat(world.get(SENDER)).isNull();
    assertThat(world.get(PAYMASTER).getBalance()).isEqualTo(Wei.of(5_000));
    assertThat(context.payer()).isNull();
  }

  @Test
  void keyedNoncesAreConsumedAndChargedPerFirstUse() {
    final UInt256 freshKey = UInt256.valueOf(5);
    final UInt256 usedKey = UInt256.valueOf(7);
    account(SENDER, 3, Wei.of(5_000));
    world
        .getOrCreate(KeyedNonces.NONCE_MANAGER)
        .setStorageValue(KeyedNonces.nonceSlot(SENDER, usedKey), UInt256.valueOf(4));
    final FrameTransactionContext context =
        context(List.of(freshKey, usedKey), 4L, frame(BOTH, SENDER));

    final MessageFrame messageFrame = messageFrame(context, SENDER, STATE_GAS.storageSetStateGas());
    assertThat(ApproveOperation.attemptApproval(messageFrame, context, BOTH, STATE_GAS))
        .isEqualTo(ApprovalResult.SUCCESS);

    // one first use: only the fresh key creates a slot
    assertThat(messageFrame.getStateGasReservoir()).isZero();
    assertThat(messageFrame.getStateGasUsed()).isEqualTo(STATE_GAS.storageSetStateGas());
    final Account manager = world.get(KeyedNonces.NONCE_MANAGER);
    assertThat(manager.getStorageValue(KeyedNonces.nonceSlot(SENDER, freshKey)))
        .isEqualTo(UInt256.valueOf(5));
    assertThat(manager.getStorageValue(KeyedNonces.nonceSlot(SENDER, usedKey)))
        .isEqualTo(UInt256.valueOf(5));
    // keyed nonces leave the account nonce alone
    assertThat(world.get(SENDER).getNonce()).isEqualTo(3L);
    assertThat(world.get(SENDER).getBalance()).isEqualTo(Wei.of(4_000));
    assertThat(context.payer()).isEqualTo(SENDER);
  }

  @Test
  void keyedNoncesWithoutStateGasForEveryFirstUseHalt() {
    final UInt256 firstKey = UInt256.valueOf(5);
    final UInt256 secondKey = UInt256.valueOf(7);
    account(SENDER, 3, Wei.of(5_000));
    final FrameTransactionContext context =
        context(List.of(firstKey, secondKey), 0L, frame(BOTH, SENDER));

    final MessageFrame messageFrame =
        messageFrame(context, SENDER, 2 * STATE_GAS.storageSetStateGas() - 1);
    assertThat(ApproveOperation.attemptApproval(messageFrame, context, BOTH, STATE_GAS))
        .isEqualTo(ApprovalResult.OUT_OF_STATE_GAS);

    assertThat(world.get(KeyedNonces.NONCE_MANAGER)).isNull();
    assertThat(context.senderApproved()).isFalse();
    assertThat(context.payer()).isNull();
    assertThat(world.get(SENDER).getBalance()).isEqualTo(Wei.of(5_000));
  }

  @Test
  void legacyNonceAtTheMaximumOverflows() {
    account(SENDER, KeyedNonces.MAX_NONCE_SEQ, Wei.of(5_000));
    final FrameTransactionContext context = legacyContext(frame(BOTH, SENDER));

    assertThat(approve(context, BOTH, 0L)).isEqualTo(ApprovalResult.NONCE_OVERFLOW);

    assertThat(context.senderApproved()).isFalse();
    assertThat(context.payer()).isNull();
    assertThat(world.get(SENDER).getBalance()).isEqualTo(Wei.of(5_000));
    assertThat(world.get(SENDER).getNonce()).isEqualTo(KeyedNonces.MAX_NONCE_SEQ);
  }

  @Test
  void legacyNonceJustBelowTheMaximumIsConsumed() {
    account(SENDER, KeyedNonces.MAX_NONCE_SEQ - 1, Wei.of(5_000));
    final FrameTransactionContext context = legacyContext(frame(BOTH, SENDER));

    assertThat(approve(context, BOTH, 0L)).isEqualTo(ApprovalResult.SUCCESS);

    assertThat(world.get(SENDER).getNonce()).isEqualTo(KeyedNonces.MAX_NONCE_SEQ);
  }

  // ---------------------------------------------------------------------------------------------
  // execute
  // ---------------------------------------------------------------------------------------------

  @Test
  void isInvalidOutsideAFrameTransaction() {
    final MessageFrame messageFrame =
        frameBuilder(SENDER, 100_000L, Bytes.EMPTY).initialStateGasReservoir(0L).build();
    pushApproveArguments(messageFrame, 0, 0, Bytes.of(EXECUTION));

    final OperationResult result = operation.execute(messageFrame, mock(EVM.class));

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INVALID_OPERATION);
    assertThat(result.getGasCost()).isZero();
    // nothing was popped
    assertThat(messageFrame.stackSize()).isEqualTo(3);
  }

  @Test
  void onlyTheFrameTargetMayApprove() {
    final FrameTransactionContext context = legacyContext(frame(BOTH, SENDER));
    final MessageFrame messageFrame = messageFrame(context, OTHER, 0L);
    pushApproveArguments(messageFrame, 0, 0, Bytes.of(EXECUTION));

    final OperationResult result = operation.execute(messageFrame, mock(EVM.class));

    assertThat(result.getHaltReason()).isNull();
    assertThat(messageFrame.getState()).isEqualTo(MessageFrame.State.REVERT);
    assertThat(context.senderApproved()).isFalse();
  }

  @Test
  void reservedScopeBitsRevert() {
    for (final Bytes scope : List.of(Bytes.of(0x04), Bytes.of(0x01, 0x02))) {
      final FrameTransactionContext context = legacyContext(frame(BOTH, SENDER));
      final MessageFrame messageFrame = messageFrame(context, SENDER, 0L);
      pushApproveArguments(messageFrame, 0, 0, scope);

      final OperationResult result = operation.execute(messageFrame, mock(EVM.class));

      assertThat(result.getHaltReason()).isNull();
      assertThat(messageFrame.getState()).isEqualTo(MessageFrame.State.REVERT);
      assertThat(context.senderApproved()).isFalse();
    }
  }

  @Test
  void approvalReturnsTheMemoryRegionAndPaysForItsExpansion() {
    final FrameTransactionContext context = legacyContext(frame(BOTH, SENDER));
    final MessageFrame messageFrame = messageFrame(context, SENDER, 0L);
    final long expansion = GAS_CALCULATOR.memoryExpansionGasCost(messageFrame, 0, 64);
    pushApproveArguments(messageFrame, 0, 64, Bytes.of(EXECUTION));

    final OperationResult result = operation.execute(messageFrame, mock(EVM.class));

    assertThat(expansion).isPositive();
    assertThat(result.getGasCost()).isEqualTo(expansion);
    assertThat(result.getHaltReason()).isNull();
    assertThat(messageFrame.getState()).isEqualTo(MessageFrame.State.CODE_SUCCESS);
    assertThat(messageFrame.getOutputData()).isEqualTo(Bytes.wrap(new byte[64]));
    assertThat(context.senderApproved()).isTrue();
  }

  @Test
  void unaffordableMemoryExpansionHaltsBeforeApproving() {
    final FrameTransactionContext context = legacyContext(frame(BOTH, SENDER));
    final MessageFrame messageFrame =
        txValuesFrameBuilder(context, SENDER, 0L).initialGas(1L).build();
    pushApproveArguments(messageFrame, 0, 64, Bytes.of(EXECUTION));

    final OperationResult result = operation.execute(messageFrame, mock(EVM.class));

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
    assertThat(context.senderApproved()).isFalse();
  }

  @Test
  void nonceOverflowHaltsTheCallFrame() {
    account(SENDER, KeyedNonces.MAX_NONCE_SEQ, Wei.of(5_000));
    final FrameTransactionContext context = legacyContext(frame(BOTH, SENDER));
    final MessageFrame messageFrame = messageFrame(context, SENDER, 0L);
    pushApproveArguments(messageFrame, 0, 0, Bytes.of(BOTH));

    final OperationResult result = operation.execute(messageFrame, mock(EVM.class));

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.NONCE_OVERFLOW);
  }

  @Test
  void unaffordableNonceStateGasHaltsWithInsufficientGas() {
    final FrameTransactionContext context =
        legacyContext(frame(EXECUTION, SENDER), frame(PAYMENT, PAYMASTER));
    account(PAYMASTER, 0, Wei.of(5_000));
    assertThat(approve(context, EXECUTION, 0L)).isEqualTo(ApprovalResult.SUCCESS);
    context.setCurrentFrameIndex(1);
    final MessageFrame messageFrame = messageFrame(context, PAYMASTER, 0L);
    pushApproveArguments(messageFrame, 0, 0, Bytes.of(PAYMENT));

    final OperationResult result = operation.execute(messageFrame, mock(EVM.class));

    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_GAS);
    assertThat(context.payer()).isNull();
  }

  // ---------------------------------------------------------------------------------------------
  // through the Bogota EVM
  // ---------------------------------------------------------------------------------------------

  static Stream<Named<EvmConfiguration>> interpreters() {
    return Stream.of(Named.of("v1", EvmConfiguration.DEFAULT), Named.of("v2", V2));
  }

  @ParameterizedTest
  @MethodSource("interpreters")
  void bogotaRunsApproveInsideAFrameTransaction(final EvmConfiguration configuration) {
    account(SENDER, 3, Wei.of(5_000));
    final FrameTransactionContext context = legacyContext(frame(BOTH, SENDER));
    final EVM evm = MainnetEVMs.bogota(GAS_CALCULATOR, BigInteger.ONE, configuration);
    final MessageFrame messageFrame =
        txValuesFrameBuilder(context, SENDER, 0L)
            .worldUpdater(world.updater())
            .code(new Code(APPROVE_BOTH))
            .enableEvmV2(configuration.enableEvmV2())
            .build();

    process(evm, messageFrame);

    assertThat(messageFrame.getState()).isEqualTo(MessageFrame.State.COMPLETED_SUCCESS);
    assertThat(context.senderApproved()).isTrue();
    assertThat(context.payer()).isEqualTo(SENDER);
    assertThat(world.get(SENDER).getBalance()).isEqualTo(Wei.of(4_000));
    assertThat(world.get(SENDER).getNonce()).isEqualTo(4L);
  }

  @ParameterizedTest
  @MethodSource("interpreters")
  void bogotaHaltsOnApproveOutsideAFrameTransaction(final EvmConfiguration configuration) {
    account(SENDER, 3, Wei.of(5_000));
    final EVM evm = MainnetEVMs.bogota(GAS_CALCULATOR, BigInteger.ONE, configuration);
    final MessageFrame messageFrame =
        frameBuilder(SENDER, 100_000L, APPROVE_BOTH)
            .worldUpdater(world.updater())
            .enableEvmV2(configuration.enableEvmV2())
            .initialStateGasReservoir(0L)
            .build();

    process(evm, messageFrame);

    assertThat(messageFrame.getState()).isEqualTo(MessageFrame.State.COMPLETED_FAILED);
    assertThat(messageFrame.getExceptionalHaltReason())
        .contains(ExceptionalHaltReason.INVALID_OPERATION);
    assertThat(messageFrame.getRemainingGas()).isZero();
    assertThat(world.get(SENDER).getNonce()).isEqualTo(3L);
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  private void account(final Address address, final long nonce, final Wei balance) {
    world.getOrCreate(address).setNonce(nonce);
    world.getOrCreate(address).setBalance(balance);
  }

  private static FrameInfo frame(final int flags, final Address target) {
    return new FrameInfo(
        FrameTransactionContext.MODE_VERIFY, flags, target, 100_000L, 0L, Wei.ZERO, Bytes.EMPTY);
  }

  private static FrameTransactionContext legacyContext(final FrameInfo... frames) {
    return context(KeyedNonces.LEGACY_NONCE_KEYS, 0L, frames);
  }

  private static FrameTransactionContext context(
      final List<UInt256> nonceKeys, final long nonce, final FrameInfo... frames) {
    return new FrameTransactionContext(
        List.of(frames),
        List.of(),
        SENDER,
        Bytes32.ZERO,
        nonce,
        nonceKeys,
        0L,
        Wei.ONE,
        Wei.ONE,
        Wei.ZERO,
        MAX_COST);
  }

  private ApprovalResult approve(
      final FrameTransactionContext context, final int scope, final long stateGasReservoir) {
    final MessageFrame messageFrame =
        messageFrame(context, context.currentFrame().resolvedTarget(), stateGasReservoir);
    return ApproveOperation.attemptApproval(messageFrame, context, scope, STATE_GAS);
  }

  private MessageFrame messageFrame(
      final FrameTransactionContext context,
      final Address recipient,
      final long stateGasReservoir) {
    return txValuesFrameBuilder(context, recipient, stateGasReservoir).build();
  }

  private MessageFrame.Builder txValuesFrameBuilder(
      final FrameTransactionContext context,
      final Address recipient,
      final long stateGasReservoir) {
    final TxValues txValues =
        TxValues.forTransaction(
            (__, ___) -> Hash.ZERO,
            1024,
            UndoSet.of(new TreeSet<>()),
            FrameTransactionContext.ENTRY_POINT,
            Wei.ONE,
            Wei.ONE,
            new ToyBlockValues(),
            Address.ZERO,
            Optional.empty(),
            stateGasReservoir);
    txValues.setFrameTransactionContext(context);
    return frameBuilder(recipient, 100_000L, Bytes.EMPTY).txValues(txValues);
  }

  private MessageFrame.Builder frameBuilder(
      final Address recipient, final long gas, final Bytes code) {
    return MessageFrame.builder()
        .type(MessageFrame.Type.MESSAGE_CALL)
        .worldUpdater(world)
        .initialGas(gas)
        .address(recipient)
        .contract(recipient)
        .originator(FrameTransactionContext.ENTRY_POINT)
        .gasPrice(Wei.ONE)
        .blobGasPrice(Wei.ONE)
        .inputData(Bytes.EMPTY)
        .sender(FrameTransactionContext.ENTRY_POINT)
        .value(Wei.ZERO)
        .apparentValue(Wei.ZERO)
        .code(new Code(code))
        .blockValues(new ToyBlockValues())
        .completer(__ -> {})
        .miningBeneficiary(Address.ZERO)
        .blockHashLookup((__, ___) -> Hash.ZERO);
  }

  private static void pushApproveArguments(
      final MessageFrame messageFrame, final int offset, final int length, final Bytes scope) {
    messageFrame.pushStackItem(scope);
    messageFrame.pushStackItem(Bytes.of(length));
    messageFrame.pushStackItem(Bytes.of(offset));
  }

  private static void process(final EVM evm, final MessageFrame messageFrame) {
    final MessageCallProcessor processor =
        new MessageCallProcessor(evm, new PrecompileContractRegistry());
    while (!messageFrame.getMessageFrameStack().isEmpty()) {
      processor.process(
          messageFrame.getMessageFrameStack().peekFirst(), OperationTracer.NO_TRACING);
    }
  }
}
