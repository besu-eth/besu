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
import static org.hyperledger.besu.evm.frame.ExceptionalHaltReason.INSUFFICIENT_GAS;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.ConstantinopleGasCalculator;
import org.hyperledger.besu.evm.gascalculator.Eip8037StateGasCostCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.testutils.FakeBlockValues;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.v2.StackArithmetic;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.List;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Mirrors {@code SStoreOperationTest} for the V2 (long[] stack) SSTORE, so the EIP-2200 sentry,
 * EIP-8038 execution-gas pricing and EIP-8037 state-gas accounting stay in lockstep with V1.
 */
class SStoreOperationV2Test {

  private static final Address ADDRESS = Address.fromHexString("0x18675309");
  private static final long BLOCK_GAS_LIMIT = 36_000_000L;
  private static final GasCalculator constantinopleCalc = new ConstantinopleGasCalculator();

  static Iterable<Arguments> data() {
    return List.of(
        Arguments.of(SStoreOperationV2.FRONTIER_MINIMUM, 200L, 200L, null),
        Arguments.of(SStoreOperationV2.EIP_1706_MINIMUM, 200L, 200L, INSUFFICIENT_GAS),
        Arguments.of(SStoreOperationV2.FRONTIER_MINIMUM, 10_000L, 10_000L, null),
        Arguments.of(SStoreOperationV2.EIP_1706_MINIMUM, 10_000L, 10_000L, null),
        Arguments.of(SStoreOperationV2.FRONTIER_MINIMUM, 10_000L, 200L, null),
        Arguments.of(SStoreOperationV2.EIP_1706_MINIMUM, 10_000L, 200L, INSUFFICIENT_GAS));
  }

  /** Pushes a 256-bit word onto the frame's V2 stack, making it the new top item. */
  private static void push(final MessageFrame frame, final UInt256 value) {
    final int newTop = frame.stackTopV2() + 1;
    frame.setTopV2(newTop);
    final byte[] bytes = value.toArrayUnsafe();
    StackArithmetic.fromBytesAt(frame.stackDataV2(), newTop, 0, bytes, 0, bytes.length);
  }

  private static FakeBlockValues blockValues() {
    return new FakeBlockValues(1337) {
      @Override
      public long getGasLimit() {
        return BLOCK_GAS_LIMIT;
      }
    };
  }

  /** Builds a tx-context frame over {@code toyWorld}, with SSTORE operands already on the stack. */
  private static MessageFrame frameFor(
      final ToyWorld toyWorld,
      final WorldUpdater txUpdater,
      final long initialGas,
      final UInt256 key,
      final UInt256 newValue) {
    return new TestMessageFrameBuilderV2()
        .address(ADDRESS)
        .worldUpdater(txUpdater == null ? toyWorld.updater() : txUpdater)
        .blockValues(blockValues())
        .initialGas(initialGas)
        // SSTORE pops key from the top of the stack, then the new value.
        .pushStackItem(newValue)
        .pushStackItem(key)
        .build();
  }

  /** Commits an account with balance 1 and an optional pre-existing storage value. */
  private static ToyWorld worldWithAccount(final UInt256 key, final UInt256 storedValue) {
    final ToyWorld toyWorld = new ToyWorld();
    final WorldUpdater baseUpdater = toyWorld.updater();
    final var baseAccount = baseUpdater.getOrCreate(ADDRESS);
    baseAccount.setBalance(Wei.of(1));
    if (storedValue != null) {
      baseAccount.setStorageValue(key, storedValue);
    }
    baseUpdater.commit();
    return toyWorld;
  }

  @ParameterizedTest(
      name = "{index}: minimum gas {0}, initial gas {1}, remaining gas {2}, expected halt {3}")
  @MethodSource("data")
  void storeOperation(
      final long minimumGasAvailable,
      final long initialGas,
      final long remainingGas,
      final ExceptionalHaltReason expectedHalt) {
    final SStoreOperationV2 operation =
        new SStoreOperationV2(constantinopleCalc, minimumGasAvailable);
    final ToyWorld toyWorld = worldWithAccount(UInt256.ONE, null);
    final MessageFrame frame =
        frameFor(toyWorld, toyWorld.updater(), initialGas, UInt256.ONE, UInt256.ZERO);
    frame.setGasRemaining(remainingGas);

    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(expectedHalt);
  }

  @Test
  void shouldHaltOnStackUnderflow() {
    final SStoreOperationV2 operation =
        new SStoreOperationV2(constantinopleCalc, SStoreOperationV2.FRONTIER_MINIMUM);
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .worldUpdater(worldWithAccount(UInt256.ONE, null).updater())
            .pushStackItem(UInt256.ONE)
            .build();

    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
  }

  @Test
  void shouldHaltInStaticContext() {
    final SStoreOperationV2 operation =
        new SStoreOperationV2(constantinopleCalc, SStoreOperationV2.FRONTIER_MINIMUM);
    final MessageFrame frame =
        new TestMessageFrameBuilderV2()
            .address(ADDRESS)
            .worldUpdater(worldWithAccount(UInt256.ONE, null).updater())
            .isStatic(true)
            .pushStackItem(UInt256.valueOf(42))
            .pushStackItem(UInt256.ONE)
            .build();

    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    assertThat(frame.stackTopV2()).isZero();
  }

  @Test
  void sstoreZeroToNonzeroTracksStateGasWithAmsterdam() {
    final SStoreOperationV2 operation =
        new SStoreOperationV2(new AmsterdamGasCalculator(), SStoreOperationV2.EIP_1706_MINIMUM);
    final ToyWorld toyWorld = worldWithAccount(UInt256.ONE, null);
    final WorldUpdater txUpdater = toyWorld.updater();

    // key=1, newValue=42 (0 -> nonzero triggers state gas)
    final MessageFrame frame =
        frameFor(toyWorld, txUpdater, 200_000L, UInt256.ONE, UInt256.valueOf(42));

    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();
    // EIP-8038 cold 0 -> nonzero: 100 warm base + 10,000 STORAGE_WRITE + 2,000 cold surcharge.
    assertThat(result.getGasCost()).isEqualTo(12_100L);
    assertThat(frame.stackTopV2()).isZero();

    final long expectedStateGas = new Eip8037StateGasCostCalculator().storageSetStateGas();
    assertThat(frame.getStateGasUsed()).isEqualTo(expectedStateGas);
    assertThat(txUpdater.getAccount(ADDRESS).getStorageValue(UInt256.ONE))
        .isEqualTo(UInt256.valueOf(42));
  }

  @Test
  void sstoreNonzeroToNonzeroDoesNotTrackStateGas() {
    final SStoreOperationV2 operation =
        new SStoreOperationV2(new AmsterdamGasCalculator(), SStoreOperationV2.EIP_1706_MINIMUM);
    final ToyWorld toyWorld = worldWithAccount(UInt256.ONE, UInt256.valueOf(99));

    // key=1, newValue=42 (nonzero -> nonzero, no state gas)
    final MessageFrame frame = frameFor(toyWorld, null, 100_000L, UInt256.ONE, UInt256.valueOf(42));

    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();

    // No state gas for nonzero -> nonzero
    assertThat(frame.getStateGasUsed()).isEqualTo(0L);
  }

  @Test
  void sstoreZeroToNonzeroToZeroRefundsStateGas() {
    final SStoreOperationV2 operation =
        new SStoreOperationV2(new AmsterdamGasCalculator(), SStoreOperationV2.EIP_1706_MINIMUM);
    final ToyWorld toyWorld = worldWithAccount(UInt256.ONE, null);

    // First SSTORE: key=1, value=42 (0 -> nonzero, triggers state gas)
    final MessageFrame frame = frameFor(toyWorld, null, 100_000L, UInt256.ONE, UInt256.valueOf(42));
    frame.setStateGasReservoir(100_000L);

    final OperationResult result1 = operation.execute(frame, null);
    assertThat(result1.getHaltReason()).isNull();

    final long expectedStateGas = new Eip8037StateGasCostCalculator().storageSetStateGas();
    assertThat(frame.getStateGasUsed()).isEqualTo(expectedStateGas);

    // Second SSTORE: key=1, value=0 (nonzero -> 0, original=0 triggers state gas refund)
    push(frame, UInt256.ZERO);
    push(frame, UInt256.ONE);
    final OperationResult result2 = operation.execute(frame, null);
    assertThat(result2.getHaltReason()).isNull();

    // EIP-8037: state gas refund is credited directly to state_gas_reservoir (not refund_counter,
    // bypassing the 20% cap) and stateGasUsed is decremented. EIP-8038: the execution-gas refund
    // for 0->X->0 is the flat STORAGE_WRITE (10,000) charged on the first change, refunded when the
    // slot is restored to its original (zero) value; it still goes via refund_counter.
    assertThat(frame.getGasRefund()).isEqualTo(10_000L);
    assertThat(frame.getStateGasUsed()).isZero();
    assertThat(frame.getStateGasReservoir()).isEqualTo(100_000L);
  }

  @Test
  void sstoreNonzeroToZeroOriginalNonzeroNoStateGasRefund() {
    final SStoreOperationV2 operation =
        new SStoreOperationV2(new AmsterdamGasCalculator(), SStoreOperationV2.EIP_1706_MINIMUM);
    final ToyWorld toyWorld = worldWithAccount(UInt256.ONE, UInt256.valueOf(99));

    // SSTORE key=1, value=0 (nonzero -> 0, original nonzero: no state gas)
    final MessageFrame frame = frameFor(toyWorld, null, 100_000L, UInt256.ONE, UInt256.ZERO);

    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();

    // No state gas for clearing when original was nonzero
    assertThat(frame.getStateGasUsed()).isEqualTo(0L);
    // EIP-8038: storage-clear refund = (STORAGE_WRITE 10,000 + COLD_STORAGE_ACCESS 2,100) * 4800 /
    // 5000 = 11,616 (replaces the London SSTORE_CLEARS_SCHEDULE of 4,800).
    assertThat(frame.getGasRefund()).isEqualTo(11_616L);
  }

  @Test
  void sstoreStateGasSpillsFromReservoirToGasRemaining() {
    final SStoreOperationV2 operation =
        new SStoreOperationV2(new AmsterdamGasCalculator(), SStoreOperationV2.EIP_1706_MINIMUM);
    final ToyWorld toyWorld = worldWithAccount(UInt256.ONE, null);

    // EIP-8038: the execution-gas SSTORE cost for a cold 0->nonzero set is 12,100 (100 warm base +
    // 10,000 STORAGE_WRITE + 2,000 cold surcharge). The frame must retain enough gas after that
    // deduction to absorb the state-gas spill, so initialGas is set well above it.
    final MessageFrame frame = frameFor(toyWorld, null, 200_000L, UInt256.ONE, UInt256.valueOf(42));

    // Set reservoir to less than what the SSTORE will need
    frame.setStateGasReservoir(10_000L);
    final long gasBeforeSstore = frame.getRemainingGas();

    // SSTORE 0 -> nonzero: state gas demand exceeds the 10k reservoir, the excess must spill to
    // execution gas.
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();

    final long expectedStateGas = new Eip8037StateGasCostCalculator().storageSetStateGas();
    final long expectedSpill = expectedStateGas - 10_000L;

    // Reservoir fully drained
    assertThat(frame.getStateGasReservoir()).isEqualTo(0L);
    // Total state gas consumed
    assertThat(frame.getStateGasUsed()).isEqualTo(expectedStateGas);
    // gasRemaining decreased by the spill amount only (execution-gas SSTORE cost is deducted by the
    // EVM after execute returns, not by the operation itself)
    final long expectedRemainingGas = gasBeforeSstore - expectedSpill;
    assertThat(frame.getRemainingGas()).isEqualTo(expectedRemainingGas);
  }

  @Test
  void sstoreStateGasExceedingReservoirAndGasRemainingHalts() {
    final SStoreOperationV2 operation =
        new SStoreOperationV2(new AmsterdamGasCalculator(), SStoreOperationV2.EIP_1706_MINIMUM);
    final ToyWorld toyWorld = worldWithAccount(UInt256.ONE, null);
    final WorldUpdater txUpdater = toyWorld.updater();

    // Enough gas for the 12,100 execution cost, but not for the state gas spill on top of it.
    final MessageFrame frame =
        frameFor(toyWorld, txUpdater, 13_000L, UInt256.ONE, UInt256.valueOf(42));
    frame.setStateGasReservoir(0L);

    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(INSUFFICIENT_GAS);
    assertThat(result.getGasCost()).isEqualTo(12_100L);
    // The write must not have happened.
    assertThat(txUpdater.getAccount(ADDRESS).getStorageValue(UInt256.ONE)).isEqualTo(UInt256.ZERO);
  }
}
