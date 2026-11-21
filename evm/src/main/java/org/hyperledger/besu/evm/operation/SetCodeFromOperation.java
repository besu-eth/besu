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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.Words;

import java.util.Set;

import org.apache.tuweni.bytes.Bytes;

/**
 * The SETCODEFROM operation (EIP-8298), which replaces the code of the executing account with the
 * code of another account.
 *
 * <p>The executing account adopts the code hash of the source, which must exist, hold code that
 * does not start with {@code 0xEF}, and not have been created in the current transaction. Otherwise
 * nothing changes. It pushes 1 for a valid source and 0 otherwise. The running frame keeps
 * executing the code it loaded; later calls to the account run the adopted code. The code of a
 * system contract is never replaced.
 */
public class SetCodeFromOperation extends AbstractOperation {

  /** The SETCODEFROM opcode. */
  public static final int OPCODE = 0xA7;

  /**
   * The contracts the protocol calls from the system address while processing a block, as fixed by
   * EIP-4788, EIP-2935, EIP-7002, EIP-7251 and EIP-8282. The deposit contract is not among them,
   * because it is never called.
   */
  private static final Set<Address> SYSTEM_CONTRACTS =
      Set.of(
          Address.fromHexString("0x000F3df6D732807Ef1319fB7B8bB8522d0Beac02"),
          Address.fromHexString("0x0000F90827F1C53a10cb7A02335B175320002935"),
          Address.fromHexString("0x00000961Ef480Eb55e80D19ad83579A64c007002"),
          Address.fromHexString("0x0000BBdDc7CE488642fb579F8B00f3a590007251"),
          Address.fromHexString("0x0000BFF46984E3725691FA540A8C7589300D8282"),
          Address.fromHexString("0x000064D678505AD48F8CCB093BC65613800E8282"));

  /**
   * Instantiates a new SETCODEFROM operation.
   *
   * @param gasCalculator the gas calculator
   */
  public SetCodeFromOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "SETCODEFROM", 1, 1, gasCalculator);
  }

  /**
   * Returns the accounts whose code SETCODEFROM never replaces.
   *
   * @return the system contracts
   */
  public Set<Address> getSystemContracts() {
    return SYSTEM_CONTRACTS;
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    if (frame.isStatic()) {
      return new OperationResult(0, ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    }
    if (SYSTEM_CONTRACTS.contains(frame.getRecipientAddress())) {
      return new OperationResult(0, ExceptionalHaltReason.CODE_CHANGE_IN_SYSTEM_CONTRACT);
    }
    final Address source = Words.toAddress(frame.popStackItem());

    // Looking the source up and inspecting its code is paid before the source is accessed.
    final long accessCost = accessCost(frame, source, gasCalculator());
    if (frame.getRemainingGas() < accessCost) {
      return new OperationResult(accessCost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }
    frame.warmUpAddress(source);

    final Account sourceAccount = getAccount(source, frame);
    if (!isAdoptable(frame, source, sourceAccount)) {
      frame.pushStackItem(Bytes.EMPTY);
      return new OperationResult(accessCost, null);
    }

    // A valid source also pays to compare the code hashes, and to write when they differ.
    long cost = accessCost + gasCalculator().getWarmStorageReadCost();
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }
    final MutableAccount currentAccount = getMutableAccount(frame.getRecipientAddress(), frame);
    if (!currentAccount.getCodeHash().equals(sourceAccount.getCodeHash())) {
      cost += gasCalculator().getAccountWriteGasCost();
      if (frame.getRemainingGas() < cost) {
        return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
      }
      adoptCode(frame, currentAccount, sourceAccount);
    }
    frame.pushStackItem(BYTES_ONE);
    return new OperationResult(cost, null);
  }

  /**
   * Returns the state-independent cost: the access to the source plus the inspection of its code.
   *
   * @param frame the frame executing SETCODEFROM
   * @param source the account whose code would be adopted
   * @param gasCalculator the gas calculator
   * @return the cost charged on every attempt
   */
  public static long accessCost(
      final MessageFrame frame, final Address source, final GasCalculator gasCalculator) {
    final boolean sourceIsWarm = frame.isAddressWarm(source) || gasCalculator.isPrecompile(source);
    return (sourceIsWarm
            ? gasCalculator.getWarmStorageReadCost()
            : gasCalculator.getColdAccountAccessCost())
        + gasCalculator.getWarmStorageReadCost();
  }

  /**
   * Returns whether the executing account can adopt the code of the source: the source exists,
   * holds code that does not start with {@code 0xEF}, and was not created in this transaction.
   *
   * @param frame the frame executing SETCODEFROM
   * @param source the address of the source
   * @param sourceAccount the source account, or null if it does not exist
   * @return true if the source is valid
   */
  public static boolean isAdoptable(
      final MessageFrame frame, final Address source, final Account sourceAccount) {
    return sourceAccount != null
        && !Hash.EMPTY.equals(sourceAccount.getCodeHash())
        && sourceAccount.getCode().get(0) != (byte) 0xEF
        && !frame.wasCreatedInTransaction(source);
  }

  /**
   * Sets the code of the executing account to the code of the source, and records the account so
   * the block access list carries the change as the code hash.
   *
   * @param frame the frame executing SETCODEFROM
   * @param currentAccount the executing account
   * @param sourceAccount the valid source
   */
  public static void adoptCode(
      final MessageFrame frame, final MutableAccount currentAccount, final Account sourceAccount) {
    currentAccount.setCode(sourceAccount.getCode());
    frame.addAdoptedCode(currentAccount.getAddress());
  }
}
