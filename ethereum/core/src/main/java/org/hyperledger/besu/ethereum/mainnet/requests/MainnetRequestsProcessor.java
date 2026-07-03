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

import org.hyperledger.besu.datatypes.RequestType;

public class MainnetRequestsProcessor {

  public static RequestProcessorCoordinator pragueRequestsProcessors(
      final RequestContractAddresses requestContractAddresses) {
    return pragueRequestsProcessorsBuilder(requestContractAddresses).build();
  }

  /**
   * Amsterdam request processors: the Prague set plus the EIP-8282 builder deposit (0x03) and
   * builder exit (0x04) requests, each collected via an empty-data system call to its fixed
   * predeploy.
   */
  public static RequestProcessorCoordinator amsterdamRequestsProcessors(
      final RequestContractAddresses requestContractAddresses) {
    return pragueRequestsProcessorsBuilder(requestContractAddresses)
        .addProcessor(
            RequestType.BUILDER_DEPOSIT,
            new SystemCallRequestProcessor(
                // eth_config key name per EELS amsterdam fork.py (EIP-8282); note builders use
                // *_CONTRACT_ADDRESS, unlike withdrawal/consolidation's
                // *_REQUEST_PREDEPLOY_ADDRESS.
                "BUILDER_DEPOSIT_CONTRACT_ADDRESS",
                requestContractAddresses.getBuilderDepositRequestContractAddress(),
                RequestType.BUILDER_DEPOSIT))
        .addProcessor(
            RequestType.BUILDER_EXIT,
            new SystemCallRequestProcessor(
                "BUILDER_EXIT_CONTRACT_ADDRESS",
                requestContractAddresses.getBuilderExitRequestContractAddress(),
                RequestType.BUILDER_EXIT))
        .build();
  }

  private static RequestProcessorCoordinator.Builder pragueRequestsProcessorsBuilder(
      final RequestContractAddresses requestContractAddresses) {
    return new RequestProcessorCoordinator.Builder()
        .addProcessor(
            RequestType.WITHDRAWAL,
            new SystemCallRequestProcessor(
                "WITHDRAWAL_REQUEST_PREDEPLOY_ADDRESS",
                requestContractAddresses.getWithdrawalRequestContractAddress(),
                RequestType.WITHDRAWAL))
        .addProcessor(
            RequestType.CONSOLIDATION,
            new SystemCallRequestProcessor(
                "CONSOLIDATION_REQUEST_PREDEPLOY_ADDRESS",
                requestContractAddresses.getConsolidationRequestContractAddress(),
                RequestType.CONSOLIDATION))
        .addProcessor(
            RequestType.DEPOSIT,
            new DepositRequestProcessor(requestContractAddresses.getDepositContractAddress()));
  }
}
