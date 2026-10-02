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
package org.hyperledger.besu.ethereum.mainnet.requests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;

import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

public class RequestContractAddressesTest {

  @Test
  public void defaultsWithdrawalAndConsolidationAddressesWhenAbsent() {
    final String json =
        """
        {"config": {
          "depositContractAddress": "0x00000000219ab540356cBB839Cbe05303d7705Fa"
        }}
        """;

    final RequestContractAddresses addresses =
        RequestContractAddresses.fromGenesis(GenesisConfig.fromConfig(json).getConfigOptions());

    assertThat(addresses.getWithdrawalRequestContractAddress())
        .isEqualTo(RequestContractAddresses.DEFAULT_WITHDRAWAL_REQUEST_CONTRACT_ADDRESS);
    assertThat(addresses.getConsolidationRequestContractAddress())
        .isEqualTo(RequestContractAddresses.DEFAULT_CONSOLIDATION_REQUEST_CONTRACT_ADDRESS);
    assertThat(addresses.getDepositContractAddress())
        .isEqualTo(Address.fromHexString("0x00000000219ab540356cBB839Cbe05303d7705Fa"));
  }

  @Test
  public void configuredRequestAddressesOverrideDefaults() {
    final String json =
        """
        {"config": {
          "depositContractAddress": "0x0000000000000000000000000000000000000001",
          "withdrawalRequestContractAddress": "0x0000000000000000000000000000000000000002",
          "consolidationRequestContractAddress": "0x0000000000000000000000000000000000000003"
        }}
        """;

    final RequestContractAddresses addresses =
        RequestContractAddresses.fromGenesis(GenesisConfig.fromConfig(json).getConfigOptions());

    assertThat(addresses.getWithdrawalRequestContractAddress())
        .isEqualTo(Address.fromHexString("0x0000000000000000000000000000000000000002"));
    assertThat(addresses.getConsolidationRequestContractAddress())
        .isEqualTo(Address.fromHexString("0x0000000000000000000000000000000000000003"));
  }

  @Test
  public void depositContractAddressIsRequired() {
    assertThatExceptionOfType(NoSuchElementException.class)
        .isThrownBy(
            () ->
                RequestContractAddresses.fromGenesis(
                    GenesisConfig.fromConfig("{\"config\": {}}").getConfigOptions()))
        .withMessageContaining("Deposit Contract Address not found");
  }
}
