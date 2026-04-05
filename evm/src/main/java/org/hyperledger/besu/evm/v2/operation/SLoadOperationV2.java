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

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.v2.StackArithmetic;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * EVM v2 SLOAD operation using long[] stack representation.
 *
 * <p>Pops a storage key from the stack, loads the value from the contract's storage, and pushes the
 * result back. Applies warm/cold access costs per EIP-2929.
 */
public class SLoadOperationV2 extends AbstractOperationV2 {

  private final long warmCost;
  private final long coldCost;
  private final OperationResult warmSuccess;
  private final OperationResult coldSuccess;

  /**
   * Instantiates a new SLoad operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SLoadOperationV2(final GasCalculator gasCalculator) {
    super(0x54, "SLOAD", 1, 1, gasCalculator);
    final long baseCost = gasCalculator.getSloadOperationGasCost();
    warmCost = baseCost + gasCalculator.getWarmStorageReadCost();
    coldCost = baseCost + gasCalculator.getColdSloadCost();
    warmSuccess = new OperationResult(warmCost, null);
    coldSuccess = new OperationResult(coldCost, null);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    return staticOperation(
        frame, frame.stackDataV2(), warmCost, coldCost, warmSuccess, coldSuccess);
  }

  /**
   * Execute the SLOAD opcode on the v2 long[] stack.
   *
   * <p>Called from EVM.java dispatch. Computes costs from the gas calculator on first call; uses
   * pre-cached results when called via the instance {@code execute()} method.
   *
   * @param frame the message frame
   * @param s the stack as a long[] array
   * @param gasCalculator the gas calculator
   * @return the operation result
   */
  public static OperationResult staticOperation(
      final MessageFrame frame, final long[] s, final GasCalculator gasCalculator) {
    final long baseCost = gasCalculator.getSloadOperationGasCost();
    final long warmCost = baseCost + gasCalculator.getWarmStorageReadCost();
    final long coldCost = baseCost + gasCalculator.getColdSloadCost();
    return staticOperation(
        frame,
        s,
        warmCost,
        coldCost,
        new OperationResult(warmCost, null),
        new OperationResult(coldCost, null));
  }

  private static OperationResult staticOperation(
      final MessageFrame frame,
      final long[] s,
      final long warmCost,
      final long coldCost,
      final OperationResult warmSuccess,
      final OperationResult coldSuccess) {
    if (!frame.stackHasItems(1)) {
      return new OperationResult(warmCost, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);
    }

    final int top = frame.stackTopV2();

    final byte[] keyBytes = new byte[32];
    StackArithmetic.toBytesAt(s, top, 0, keyBytes);
    final Bytes32 keyBytes32 = Bytes32.wrap(keyBytes);
    final UInt256 key = UInt256.fromBytes(keyBytes32);

    final boolean slotIsWarm = frame.warmUpStorage(frame.getRecipientAddress(), keyBytes32);
    final long cost = slotIsWarm ? warmCost : coldCost;

    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }

    final Account account = frame.getWorldUpdater().get(frame.getRecipientAddress());
    frame.getEip7928AccessList().ifPresent(t -> t.addTouchedAccount(frame.getRecipientAddress()));

    final UInt256 value = account == null ? UInt256.ZERO : account.getStorageValue(key);
    frame
        .getEip7928AccessList()
        .ifPresent(t -> t.addSlotAccessForAccount(frame.getRecipientAddress(), key));

    final byte[] valueBytes = value.toArrayUnsafe();
    StackArithmetic.fromBytesAt(s, top, 0, valueBytes, 0, valueBytes.length);

    return slotIsWarm ? warmSuccess : coldSuccess;
  }
}
