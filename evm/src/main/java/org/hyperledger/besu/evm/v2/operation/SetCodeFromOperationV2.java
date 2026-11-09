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

import static org.hyperledger.besu.evm.v2.operation.StackUtil.readAddressAt;
import static org.hyperledger.besu.evm.v2.operation.StackUtil.setLong;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.operation.SetCodeFromOperation;

import java.util.Set;

/**
 * The EIP-8298 SETCODEFROM operation (0xA7). The source checks and the code adoption are shared
 * with {@link SetCodeFromOperation}.
 */
public class SetCodeFromOperationV2 extends AbstractOperationV2 {

  private final Set<Address> systemContracts;

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   * @param systemContracts the accounts whose code is never replaced
   */
  public SetCodeFromOperationV2(
      final GasCalculator gasCalculator, final Set<Address> systemContracts) {
    super(SetCodeFromOperation.OPCODE, "SETCODEFROM", 1, 1, gasCalculator);
    this.systemContracts = systemContracts;
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    if (frame.isStatic()) {
      return new OperationResult(0, ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    }
    if (systemContracts.contains(frame.getRecipientAddress())) {
      return new OperationResult(0, ExceptionalHaltReason.CODE_CHANGE_IN_SYSTEM_CONTRACT);
    }
    if (!frame.stackHasItemsV2(1)) return UNDERFLOW_RESPONSE;
    final long[] stack = frame.stackDataV2();
    final int top = frame.stackTopV2();
    final Address source = readAddressAt(stack, top, 0);

    final long accessCost = SetCodeFromOperation.accessCost(frame, source, gasCalculator());
    if (frame.getRemainingGas() < accessCost) {
      return haltPopping(
          frame, 1, new OperationResult(accessCost, ExceptionalHaltReason.INSUFFICIENT_GAS));
    }
    frame.warmUpAddress(source);

    final Account sourceAccount = getAccount(source, frame);
    if (!SetCodeFromOperation.isAdoptable(frame, source, sourceAccount)) {
      setLong(0L, stack, top - 1);
      return new OperationResult(accessCost, null);
    }

    long cost = accessCost + gasCalculator().getWarmStorageReadCost();
    if (frame.getRemainingGas() < cost) {
      return haltPopping(
          frame, 1, new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS));
    }
    final MutableAccount currentAccount = getMutableAccount(frame.getRecipientAddress(), frame);
    if (!currentAccount.getCodeHash().equals(sourceAccount.getCodeHash())) {
      cost += gasCalculator().getAccountWriteGasCost();
      if (frame.getRemainingGas() < cost) {
        return haltPopping(
            frame, 1, new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS));
      }
      SetCodeFromOperation.adoptCode(frame, currentAccount, sourceAccount);
    }
    setLong(1L, stack, top - 1);
    return new OperationResult(cost, null);
  }
}
