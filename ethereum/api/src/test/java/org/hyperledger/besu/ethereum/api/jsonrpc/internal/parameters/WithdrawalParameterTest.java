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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameterTestFixture.WITHDRAWAL_PARAM_1;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.Withdrawal;

import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class WithdrawalParameterTest {

  @Test
  public void toWithdrawal() {
    Withdrawal expected =
        new Withdrawal(
            UInt64.fromHexString("0x0"),
            UInt64.fromHexString("0xffff"),
            Address.fromHexString("0x0000000000000000000000000000000000000000"),
            GWei.of(0L));
    assertThat(WITHDRAWAL_PARAM_1.toWithdrawal()).isEqualTo(expected);
  }

  @Test
  public void toWithdrawalAcceptsUpperCasePrefix() {
    final WithdrawalParameter parameter =
        new WithdrawalParameter("0x0", "0x0", "0X00000000000000000000000000000000000000Ee", "0x1");
    assertThat(parameter.toWithdrawal().getAddress())
        .isEqualTo(Address.fromHexString("0x00000000000000000000000000000000000000ee"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "0x",
        "0x1",
        "0xee",
        "0x000000000000000000000000000000000000000",
        "0x00000000000000000000000000000000000000000",
        "00000000000000000000000000000000000000ee",
        "0x00000000000000000000000000000000000000gg"
      })
  public void toWithdrawalRejectsMalformedAddress(final String address) {
    final WithdrawalParameter parameter = new WithdrawalParameter("0x0", "0x0", address, "0x1");
    assertThatThrownBy(parameter::toWithdrawal)
        .isInstanceOfSatisfying(
            InvalidJsonRpcParameters.class,
            e -> assertThat(e.getRpcErrorType()).isEqualTo(RpcErrorType.INVALID_PARAMS));
  }

  @Test
  public void toWithdrawalRejectsMissingAddress() {
    final WithdrawalParameter parameter = new WithdrawalParameter("0x0", "0x0", null, "0x1");
    assertThatThrownBy(parameter::toWithdrawal).isInstanceOf(InvalidJsonRpcParameters.class);
  }

  @Test
  public void fromWithdrawal() {
    Withdrawal withdrawal =
        new Withdrawal(
            UInt64.fromHexString("0x0"),
            UInt64.fromHexString("0xffff"),
            Address.fromHexString("0x0000000000000000000000000000000000000000"),
            GWei.of(0L));
    assertThat(WithdrawalParameter.fromWithdrawal(withdrawal)).isEqualTo(WITHDRAWAL_PARAM_1);
  }
}
