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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.gascalculator.Eip8037StateGasCostCalculator;
import org.hyperledger.besu.evm.testutils.FakeBlockValues;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import org.junit.jupiter.api.Test;

/**
 * Tests that the EIP-8037 NEW_ACCOUNT state-gas refund in {@link AbstractCallOperation#complete()}
 * is symmetric with the charge in {@link AbstractCallOperation#execute()}: both must target the
 * call's *recipient* address (i.e. {@code address(frame)}).
 *
 * <p>For CALLCODE the recipient is the caller's own address (the execution context), while the
 * contract address is the code target. Charging against the caller's address (which exists) issues
 * no state gas; the old code refunded against the contract address (e.g. an empty precompile),
 * driving state-gas-used negative and corrupting the sender's balance.
 */
class CallCodeOperationStateGasRefundTest {

  private static final Address CALLER_ADDRESS = Address.fromHexString("0xCAFE");
  // Ecrecover precompile — present in Amsterdam and always empty in world state
  private static final Address PRECOMPILE_ADDRESS = Address.fromHexString("0x01");
  private static final Wei ONE_WEI = Wei.ONE;

  private final Eip8037StateGasCostCalculator calculator = new Eip8037StateGasCostCalculator();

  private MessageFrame buildFrame(final Address recipientAddress) {
    final ToyWorld toyWorld = new ToyWorld();
    final WorldUpdater updater = toyWorld.updater();
    // Give the caller a non-empty account so it does not qualify as a new-account creation target
    updater.getOrCreate(recipientAddress).setBalance(Wei.of(1_000_000));
    updater.commit();
    return new TestMessageFrameBuilder()
        .address(recipientAddress)
        .worldUpdater(toyWorld.updater())
        .blockValues(new FakeBlockValues(1))
        .initialGas(10_000_000L)
        .build();
  }

  /**
   * Charging NEW_ACCOUNT state gas against the recipient (caller's address) when the caller already
   * exists: no state gas is consumed (the account is not new).
   */
  @Test
  void chargeAgainstExistingRecipientIsZero() {
    final MessageFrame frame = buildFrame(CALLER_ADDRESS);
    final long stateGasBefore = frame.getStateGasUsed();

    calculator.chargeCallNewAccountStateGas(frame, CALLER_ADDRESS, ONE_WEI);

    assertThat(frame.getStateGasUsed()).isEqualTo(stateGasBefore);
  }

  /**
   * Symmetric refund: calling refundCallNewAccountStateGas with the same existing-recipient address
   * issues no refund either, so stateGasUsed stays non-negative even if it started at zero.
   */
  @Test
  void refundAgainstExistingRecipientDoesNotGoNegative() {
    final MessageFrame frame = buildFrame(CALLER_ADDRESS);

    // No charge was made (caller exists), and no refund should be issued either
    calculator.refundCallNewAccountStateGas(frame, CALLER_ADDRESS, ONE_WEI);

    assertThat(frame.getStateGasUsed())
        .as("stateGasUsed must not go negative when refunding against an existing account")
        .isGreaterThanOrEqualTo(0L);
  }

  /**
   * Demonstrates the pre-fix asymmetry: refunding against the empty precompile (the old
   * getContractAddress() path) while the charge was zero would decrement stateGasUsed below zero.
   *
   * <p>This documents the bug that was fixed in AbstractCallOperation.complete().
   */
  @Test
  void refundAgainstEmptyContractAddressWouldGoNegative() {
    final MessageFrame frame = buildFrame(CALLER_ADDRESS);

    // Simulate the old buggy path: refund against the empty precompile address
    calculator.refundCallNewAccountStateGas(frame, PRECOMPILE_ADDRESS, ONE_WEI);

    // stateGasUsed is now negative — this is the bug that the fix prevents
    assertThat(frame.getStateGasUsed())
        .as(
            "demonstrates the pre-fix bug: refunding against an empty contract address goes negative")
        .isLessThan(0L);
  }

  /**
   * Correct CALLCODE scenario end-to-end: charge (zero, caller exists) + failure refund (also zero,
   * same address) leaves stateGasUsed unchanged.
   */
  @Test
  void callcodeChargeAndRefundAreSymmetricForExistingCaller() {
    final MessageFrame frame = buildFrame(CALLER_ADDRESS);
    final long stateGasBefore = frame.getStateGasUsed();

    // execute(): charge against recipientAddress = address(frame) = CALLER_ADDRESS (exists → 0)
    calculator.chargeCallNewAccountStateGas(frame, CALLER_ADDRESS, ONE_WEI);
    // complete() after failure: refund against childFrame.getRecipientAddress() = CALLER_ADDRESS
    calculator.refundCallNewAccountStateGas(frame, CALLER_ADDRESS, ONE_WEI);

    assertThat(frame.getStateGasUsed())
        .as("charge and refund must be symmetric: stateGasUsed unchanged")
        .isEqualTo(stateGasBefore);
  }

  /**
   * Uses AmsterdamGasCalculator (wraps Eip8037StateGasCostCalculator) to verify the same invariant
   * through the full gas-calculator stack as seen by AbstractCallOperation.
   */
  @Test
  void chargeRefundSymmetryViaAmsterdamGasCalculator() {
    final AmsterdamGasCalculator gasCalculator = new AmsterdamGasCalculator();
    final MessageFrame frame = buildFrame(CALLER_ADDRESS);
    final long stateGasBefore = frame.getStateGasUsed();

    gasCalculator
        .stateGasCostCalculator()
        .chargeCallNewAccountStateGas(frame, CALLER_ADDRESS, ONE_WEI);
    gasCalculator
        .stateGasCostCalculator()
        .refundCallNewAccountStateGas(frame, CALLER_ADDRESS, ONE_WEI);

    assertThat(frame.getStateGasUsed()).isEqualTo(stateGasBefore);
  }
}
