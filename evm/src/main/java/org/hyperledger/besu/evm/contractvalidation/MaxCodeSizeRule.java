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
package org.hyperledger.besu.evm.contractvalidation;

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/** The Max code size rule. */
public class MaxCodeSizeRule implements ContractValidationRule {

  private final int maxCodeSize;

  /**
   * Instantiates a new Max code size rule.
   *
   * @param maxCodeSize the max code size
   */
  MaxCodeSizeRule(final int maxCodeSize) {
    this.maxCodeSize = maxCodeSize;
  }

  @Override
  public Optional<ExceptionalHaltReason> validate(
      final Bytes contractCode, final MessageFrame frame, final EVM evm) {
    final int contractCodeSize = contractCode.size();
    if (contractCodeSize <= maxCodeSize) {
      return Optional.empty();
    } else {
      return Optional.of(ExceptionalHaltReason.CODE_TOO_LARGE);
    }
  }

  /**
   * Fluent MaxCodeSizeRule constructor of an explicit size.
   *
   * @param maxCodeSize the max code size
   * @return the contract validation rule
   * @deprecated use {@link #from(EVM)}
   */
  @Deprecated(forRemoval = true, since = "24.6.1")
  public static ContractValidationRule of(final int maxCodeSize) {
    return new MaxCodeSizeRule(maxCodeSize);
  }

  /**
   * Fluent MaxCodeSizeRule from the EVM it is working with.
   *
   * @param evm The evm to get the size rules from.
   * @return the contract validation rule
   */
  public static ContractValidationRule from(final EVM evm) {
    return from(evm.getEvmVersion(), evm.getEvmConfiguration());
  }

  /**
   * Fluent MaxCodeSizeRule from the EVM it is working with.
   *
   * @param evmspec The evm spec version to get the size rules from.
   * @param evmConfiguration The evm configuration, including overrides
   * @return the contract validation rule
   */
  public static ContractValidationRule from(
      final EvmSpecVersion evmspec, final EvmConfiguration evmConfiguration) {
    return new MaxCodeSizeRule(
        evmConfiguration.maxCodeSizeOverride().orElse(evmspec.getMaxCodeSize()));
  }
}
