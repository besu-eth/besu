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
package org.hyperledger.besu.evm.precompile;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

import java.util.Optional;

import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;

/**
 * The EIP-8151 account code restricted ECREC precompiled contract.
 *
 * <p>A successful recovery is charged an account access to the recovered address plus a warm code
 * read, warms the address, records it in the EIP-7928 block access list and reads its code. If the
 * account holds code other than an EIP-7702 delegation designation, the call succeeds with empty
 * output. A failed recovery costs only the base fee and touches no state.
 */
public class AccountCodeRestrictedECRECPrecompiledContract extends ECRECPrecompiledContract {

  /**
   * Instantiates a new account code restricted ECREC precompiled contract.
   *
   * @param gasCalculator the gas calculator
   */
  public AccountCodeRestrictedECRECPrecompiledContract(final GasCalculator gasCalculator) {
    super(gasCalculator);
  }

  @NotNull
  @Override
  public PrecompileContractResult computePrecompile(
      final Bytes input, @NotNull final MessageFrame messageFrame) {
    // The parent's result cache holds the raw recovery only: the restriction depends on state.
    final PrecompileContractResult recovery = super.computePrecompile(input, messageFrame);
    final Bytes output = recovery.output();
    if (output.isEmpty()) {
      return recovery;
    }
    final Address address = Address.wrap(output.slice(12));

    // EELS charges before warming or reading, so an out-of-gas call leaves no access behind.
    final boolean isWarm =
        messageFrame.isAddressWarm(address) || gasCalculator().isPrecompile(address);
    final long cost =
        (isWarm
                ? gasCalculator().getWarmStorageReadCost()
                : gasCalculator().getColdAccountAccessCost())
            + gasCalculator().getWarmStorageReadCost();
    if (messageFrame.getRemainingGas() < cost) {
      return PrecompileContractResult.halt(
          Bytes.EMPTY, Optional.of(ExceptionalHaltReason.INSUFFICIENT_GAS));
    }
    messageFrame.decrementRemainingGas(cost);
    messageFrame.warmUpAddress(address);
    messageFrame.getEip7928AccessList().ifPresent(t -> t.addTouchedAccount(address));

    final Account account = messageFrame.getWorldUpdater().get(address);
    final Bytes code = account == null ? Bytes.EMPTY : account.getCode();
    if (!code.isEmpty() && !CodeDelegationHelper.hasCodeDelegation(code)) {
      return PrecompileContractResult.success(Bytes.EMPTY);
    }
    return recovery;
  }
}
