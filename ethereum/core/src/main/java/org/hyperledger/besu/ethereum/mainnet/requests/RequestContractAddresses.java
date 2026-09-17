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
package org.hyperledger.besu.ethereum.mainnet.requests;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.datatypes.Address;

import java.util.NoSuchElementException;

public class RequestContractAddresses {
  /** EIP-8282 builder deposit request predeploy (spec-fixed default when not configured). */
  public static final Address DEFAULT_BUILDER_DEPOSIT_REQUEST_CONTRACT_ADDRESS =
      Address.fromHexString("0x0000BFF46984E3725691FA540A8C7589300D8282");

  /** EIP-8282 builder exit request predeploy (spec-fixed default when not configured). */
  public static final Address DEFAULT_BUILDER_EXIT_REQUEST_CONTRACT_ADDRESS =
      Address.fromHexString("0x000064D678505AD48F8CCB093BC65613800E8282");

  private final Address withdrawalRequestContractAddress;
  private final Address depositContractAddress;
  private final Address consolidationRequestContractAddress;
  private final Address builderDepositRequestContractAddress;
  private final Address builderExitRequestContractAddress;

  private RequestContractAddresses(
      final Address withdrawalRequestContractAddress,
      final Address depositContractAddress,
      final Address consolidationRequestContractAddress,
      final Address builderDepositRequestContractAddress,
      final Address builderExitRequestContractAddress) {
    this.withdrawalRequestContractAddress = withdrawalRequestContractAddress;
    this.depositContractAddress = depositContractAddress;
    this.consolidationRequestContractAddress = consolidationRequestContractAddress;
    this.builderDepositRequestContractAddress = builderDepositRequestContractAddress;
    this.builderExitRequestContractAddress = builderExitRequestContractAddress;
  }

  public static RequestContractAddresses fromGenesis(
      final GenesisConfigOptions genesisConfigOptions) {
    return new RequestContractAddresses(
        genesisConfigOptions
            .getWithdrawalRequestContractAddress()
            .orElseThrow(
                () -> new NoSuchElementException("Withdrawal Request Contract Address not found")),
        genesisConfigOptions
            .getDepositContractAddress()
            .orElseThrow(() -> new NoSuchElementException("Deposit Contract Address not found")),
        genesisConfigOptions
            .getConsolidationRequestContractAddress()
            .orElseThrow(
                () ->
                    new NoSuchElementException("Consolidation Request Contract Address not found")),
        // EIP-8282: builder request addresses are spec-fixed; the genesis config may override them.
        genesisConfigOptions
            .getBuilderDepositRequestContractAddress()
            .orElse(DEFAULT_BUILDER_DEPOSIT_REQUEST_CONTRACT_ADDRESS),
        genesisConfigOptions
            .getBuilderExitRequestContractAddress()
            .orElse(DEFAULT_BUILDER_EXIT_REQUEST_CONTRACT_ADDRESS));
  }

  public Address getWithdrawalRequestContractAddress() {
    return withdrawalRequestContractAddress;
  }

  public Address getDepositContractAddress() {
    return depositContractAddress;
  }

  public Address getConsolidationRequestContractAddress() {
    return consolidationRequestContractAddress;
  }

  public Address getBuilderDepositRequestContractAddress() {
    return builderDepositRequestContractAddress;
  }

  public Address getBuilderExitRequestContractAddress() {
    return builderExitRequestContractAddress;
  }
}
