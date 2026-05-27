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
package org.hyperledger.besu.evm.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.frame.ExceptionalHaltReason.INSUFFICIENT_GAS;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.BlockValues;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.ConstantinopleGasCalculator;
import org.hyperledger.besu.evm.gascalculator.Eip8037StateGasCostCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.testutils.FakeBlockValues;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.List;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SStoreOperationTest {

  private static final GasCalculator gasCalculator = new ConstantinopleGasCalculator();

  static Iterable<Arguments> data() {
    return List.of(
        Arguments.of(SStoreOperation.FRONTIER_MINIMUM, 200L, 200L, null),
        Arguments.of(SStoreOperation.EIP_1706_MINIMUM, 200L, 200L, INSUFFICIENT_GAS),
        Arguments.of(SStoreOperation.FRONTIER_MINIMUM, 10_000L, 10_000L, null),
        Arguments.of(SStoreOperation.EIP_1706_MINIMUM, 10_000L, 10_000L, null),
        Arguments.of(SStoreOperation.FRONTIER_MINIMUM, 10_000L, 200L, null),
        Arguments.of(SStoreOperation.EIP_1706_MINIMUM, 10_000L, 200L, INSUFFICIENT_GAS));
  }

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

  @ParameterizedTest(
      name = "{index}: minimum gas {0}, initial gas {1}, remaining gas {2}, expected halt {3}")
  @MethodSource("data")
  void storeOperation(
      final long minimumGasAvailable,
      final long initialGas,
      final long remainingGas,
      final ExceptionalHaltReason expectedHalt) {
    final SStoreOperation operation = new SStoreOperation(gasCalculator, minimumGasAvailable);
    final MessageFrame frame =
        createMessageFrame(Address.fromHexString("0x18675309"), initialGas, remainingGas);
    frame.pushStackItem(UInt256.ZERO);
    frame.pushStackItem(UInt256.fromHexString("0x01"));

    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isEqualTo(expectedHalt);
  }

  @Test
  void sstoreZeroToNonzeroTracksStateGasWithAmsterdam() {
    final GasCalculator amsterdamCalc = new AmsterdamGasCalculator();
    final SStoreOperation operation =
        new SStoreOperation(amsterdamCalc, SStoreOperation.EIP_1706_MINIMUM);

    final long blockGasLimit = 36_000_000L;
    final Address address = Address.fromHexString("0x18675309");
    final ToyWorld toyWorld = new ToyWorld();
    final WorldUpdater worldStateUpdater = toyWorld.updater();

    final MessageFrame frame =
        new TestMessageFrameBuilder()
            .address(address)
            .worldUpdater(worldStateUpdater)
            .blockValues(
                new FakeBlockValues(1337) {
                  @Override
                  public long getGasLimit() {
                    return blockGasLimit;
                  }
                })
            .initialGas(100_000L)
            .build();
    worldStateUpdater.getOrCreate(address).setBalance(Wei.of(1));
    worldStateUpdater.commit();

    // key=1, newValue=42 (0 -> nonzero triggers state gas)
    frame.pushStackItem(UInt256.valueOf(42));
    frame.pushStackItem(UInt256.ONE);

    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();

    // State gas: 32 * cpsb(36M) = 32 * 150 = 4_800
    final long expectedStateGas =
        32L * new Eip8037StateGasCostCalculator().costPerStateByte(blockGasLimit);
    assertThat(frame.getStateGasUsed()).isEqualTo(expectedStateGas);
  }

  @Test
  void sstoreNonzeroToNonzeroDoesNotTrackStateGas() {
    final GasCalculator amsterdamCalc = new AmsterdamGasCalculator();
    final SStoreOperation operation =
        new SStoreOperation(amsterdamCalc, SStoreOperation.EIP_1706_MINIMUM);

    final long blockGasLimit = 36_000_000L;
    final Address address = Address.fromHexString("0x18675309");
    final ToyWorld toyWorld = new ToyWorld();

    // Set up base state with nonzero storage
    final WorldUpdater baseUpdater = toyWorld.updater();
    final var baseAccount = baseUpdater.getOrCreate(address);
    baseAccount.setBalance(Wei.of(1));
    baseAccount.setStorageValue(UInt256.ONE, UInt256.valueOf(99));
    baseUpdater.commit();

    // Fresh updater for transaction context
    final WorldUpdater txUpdater = toyWorld.updater();
    final MessageFrame frame =
        new TestMessageFrameBuilder()
            .address(address)
            .worldUpdater(txUpdater)
            .blockValues(
                new FakeBlockValues(1337) {
                  @Override
                  public long getGasLimit() {
                    return blockGasLimit;
                  }
                })
            .initialGas(100_000L)
            .build();

    // key=1, newValue=42 (nonzero -> nonzero, no state gas)
    frame.pushStackItem(UInt256.valueOf(42));
    frame.pushStackItem(UInt256.ONE);

    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();

    // No state gas for nonzero -> nonzero
    assertThat(frame.getStateGasUsed()).isEqualTo(0L);
  }

  @Test
  void sstoreZeroToNonzeroToZeroRefundsStateGas() {
    final GasCalculator amsterdamCalc = new AmsterdamGasCalculator();
    final SStoreOperation operation =
        new SStoreOperation(amsterdamCalc, SStoreOperation.EIP_1706_MINIMUM);

    final long blockGasLimit = 36_000_000L;
    final Address address = Address.fromHexString("0x18675309");
    final ToyWorld toyWorld = new ToyWorld();

    // Set up base state with balance
    final WorldUpdater baseUpdater = toyWorld.updater();
    baseUpdater.getOrCreate(address).setBalance(Wei.of(1));
    baseUpdater.commit();

    // Fresh updater for transaction context (original values tracked from base)
    final WorldUpdater txUpdater = toyWorld.updater();
    final MessageFrame frame =
        new TestMessageFrameBuilder()
            .address(address)
            .worldUpdater(txUpdater)
            .blockValues(
                new FakeBlockValues(1337) {
                  @Override
                  public long getGasLimit() {
                    return blockGasLimit;
                  }
                })
            .initialGas(100_000L)
            .build();
    frame.setStateGasReservoir(100_000L);

    // First SSTORE: key=1, value=42 (0 -> nonzero, triggers state gas)
    frame.pushStackItem(UInt256.valueOf(42));
    frame.pushStackItem(UInt256.ONE);
    final OperationResult result1 = operation.execute(frame, null);
    assertThat(result1.getHaltReason()).isNull();

    final long expectedStateGas =
        32L * new Eip8037StateGasCostCalculator().costPerStateByte(blockGasLimit);
    assertThat(frame.getStateGasUsed()).isEqualTo(expectedStateGas); // 37,568

    // Second SSTORE: key=1, value=0 (nonzero -> 0, original=0 triggers state gas refund)
    frame.pushStackItem(UInt256.ZERO);
    frame.pushStackItem(UInt256.ONE);
    final OperationResult result2 = operation.execute(frame, null);
    assertThat(result2.getHaltReason()).isNull();

    // State gas refund (37,568) + regular SSTORE refund for 0->X->0 (2,800)
    assertThat(frame.getGasRefund()).isEqualTo(expectedStateGas + 2_800L);
    // stateGasUsed tracks gross consumption, not decremented by refunds
    assertThat(frame.getStateGasUsed()).isEqualTo(expectedStateGas);
  }

  @Test
  void sstoreNonzeroToZeroOriginalNonzeroNoStateGasRefund() {
    final GasCalculator amsterdamCalc = new AmsterdamGasCalculator();
    final SStoreOperation operation =
        new SStoreOperation(amsterdamCalc, SStoreOperation.EIP_1706_MINIMUM);

    final long blockGasLimit = 36_000_000L;
    final Address address = Address.fromHexString("0x18675309");
    final ToyWorld toyWorld = new ToyWorld();

    // Set up base state with nonzero storage
    final WorldUpdater baseUpdater = toyWorld.updater();
    final var baseAccount = baseUpdater.getOrCreate(address);
    baseAccount.setBalance(Wei.of(1));
    baseAccount.setStorageValue(UInt256.ONE, UInt256.valueOf(99));
    baseUpdater.commit();

    // Fresh updater for transaction context
    final WorldUpdater txUpdater = toyWorld.updater();
    final MessageFrame frame =
        new TestMessageFrameBuilder()
            .address(address)
            .worldUpdater(txUpdater)
            .blockValues(
                new FakeBlockValues(1337) {
                  @Override
                  public long getGasLimit() {
                    return blockGasLimit;
                  }
                })
            .initialGas(100_000L)
            .build();

    // SSTORE key=1, value=0 (nonzero -> 0, original nonzero — no state gas)
    frame.pushStackItem(UInt256.ZERO);
    frame.pushStackItem(UInt256.ONE);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();

    // No state gas for clearing when original was nonzero
    assertThat(frame.getStateGasUsed()).isEqualTo(0L);
    // Only the regular SSTORE_CLEARS_SCHEDULE refund (4,800)
    assertThat(frame.getGasRefund()).isEqualTo(4_800L);
  }

  @Test
  void sstoreStateGasSpillsFromReservoirToGasRemaining() {
    final GasCalculator amsterdamCalc = new AmsterdamGasCalculator();
    final SStoreOperation operation =
        new SStoreOperation(amsterdamCalc, SStoreOperation.EIP_1706_MINIMUM);

    final long blockGasLimit = 36_000_000L;
    final Address address = Address.fromHexString("0x18675309");
    final ToyWorld toyWorld = new ToyWorld();

    // Set up base state with balance
    final WorldUpdater baseUpdater = toyWorld.updater();
    baseUpdater.getOrCreate(address).setBalance(Wei.of(1));
    baseUpdater.commit();

    // Fresh updater for transaction context
    final WorldUpdater txUpdater = toyWorld.updater();
    final MessageFrame frame =
        new TestMessageFrameBuilder()
            .address(address)
            .worldUpdater(txUpdater)
            .blockValues(
                new FakeBlockValues(1337) {
                  @Override
                  public long getGasLimit() {
                    return blockGasLimit;
                  }
                })
            .initialGas(100_000L)
            .build();

    // Set reservoir to less than what the SSTORE will need
    frame.setStateGasReservoir(10_000L);
    final long gasBeforeSstore = frame.getRemainingGas();

    // SSTORE 0 -> nonzero: needs 37,568 state gas but only 10k in reservoir
    frame.pushStackItem(UInt256.valueOf(42));
    frame.pushStackItem(UInt256.ONE);
    final OperationResult result = operation.execute(frame, null);
    assertThat(result.getHaltReason()).isNull();

    final long expectedStateGas =
        32L * new Eip8037StateGasCostCalculator().costPerStateByte(blockGasLimit);
    final long expectedSpill = expectedStateGas - 10_000L; // 27,568

    // Reservoir fully drained
    assertThat(frame.getStateGasReservoir()).isEqualTo(0L);
    // Total state gas consumed
    assertThat(frame.getStateGasUsed()).isEqualTo(expectedStateGas);
    // gasRemaining decreased by the spill amount only (regular SSTORE cost is deducted by the
    // EVM after execute returns, not by the operation itself)
    final long expectedRemainingGas = gasBeforeSstore - expectedSpill;
    assertThat(frame.getRemainingGas()).isEqualTo(expectedRemainingGas);
  }
}
