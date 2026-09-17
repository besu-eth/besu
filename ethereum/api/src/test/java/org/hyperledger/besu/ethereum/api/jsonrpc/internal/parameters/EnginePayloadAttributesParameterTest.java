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
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameterTestFixture.WITHDRAWAL_PARAM_1;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameterTestFixture.WITHDRAWAL_PARAM_2;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;

import java.util.List;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class EnginePayloadAttributesParameterTest {

  private static final String TIMESTAMP = "0x50";
  private static final String PREV_RANDAO =
      "0xff00000000000000000000000000000000000000000000000000000000000000";
  private static final String SUGGESTED_FEE_RECIPIENT_ADDRESS =
      "0xaa00000000000000000000000000000000000000";

  @Test
  public void attributesAreConvertedFromString_WithdrawalsOmitted() {
    final EnginePayloadAttributesParameter parameter = parameterWithdrawalsOmitted();
    assertThat(parameter.getTimestamp()).isEqualTo(80L);
    assertThat(parameter.getPrevRandao())
        .isEqualTo(
            Bytes32.fromHexString(
                "0xff00000000000000000000000000000000000000000000000000000000000000"));
    assertThat(parameter.getSuggestedFeeRecipient())
        .isEqualTo(Address.fromHexString("0xaa00000000000000000000000000000000000000"));
    assertThat(parameter.getWithdrawals()).isNull();
  }

  @Test
  public void attributesAreConvertedFromString_WithdrawalsPresent() {
    final EnginePayloadAttributesParameter parameter = parameterWithdrawalsPresent();
    assertThat(parameter.getTimestamp()).isEqualTo(80L);
    assertThat(parameter.getPrevRandao())
        .isEqualTo(
            Bytes32.fromHexString(
                "0xff00000000000000000000000000000000000000000000000000000000000000"));
    assertThat(parameter.getSuggestedFeeRecipient())
        .isEqualTo(Address.fromHexString("0xaa00000000000000000000000000000000000000"));
    assertThat(parameter.getWithdrawals()).hasSize(2);
  }

  @Test
  public void serialize_WithdrawalsOmitted() {
    assertThat(parameterWithdrawalsOmitted().serialize())
        .isEqualTo(
            "{"
                + "\"timestamp\":80,"
                + "\"prevRandao\":\"0xff00000000000000000000000000000000000000000000000000000000000000\","
                + "\"suggestedFeeRecipient\":\"0xaa00000000000000000000000000000000000000\""
                + "}");
  }

  @Test
  public void serialize_WithdrawalsPresent() {
    assertThat(parameterWithdrawalsPresent().serialize())
        .isEqualTo(
            "{"
                + "\"timestamp\":80,"
                + "\"prevRandao\":\"0xff00000000000000000000000000000000000000000000000000000000000000\","
                + "\"suggestedFeeRecipient\":\"0xaa00000000000000000000000000000000000000\","
                + "\"withdrawals\":[{"
                + "\"index\":\"0x0\","
                + "\"validatorIndex\":\"0xffff\","
                + "\"address\":\"0x0000000000000000000000000000000000000000\","
                + "\"amount\":\"0x0\""
                + "},"
                + "{"
                + "\"index\":\"0x1\","
                + "\"validatorIndex\":\"0x10000\","
                + "\"address\":\"0x0100000000000000000000000000000000000000\","
                + "\"amount\":\""
                + GWei.ONE.toHexString()
                + "\""
                + "}]"
                + "}");
  }

  @Test
  public void targetGasLimitIsDecodedFromHexString() {
    final EnginePayloadAttributesParameter parameter =
        new EnginePayloadAttributesParameter(
            TIMESTAMP, PREV_RANDAO, SUGGESTED_FEE_RECIPIENT_ADDRESS, null, null, null, "0x1c9c380");
    assertThat(parameter.getTargetGasLimit()).isEqualTo(30_000_000L);
  }

  @Test
  public void serialize_TargetGasLimitPresent() {
    final EnginePayloadAttributesParameter parameter =
        new EnginePayloadAttributesParameter(
            TIMESTAMP, PREV_RANDAO, SUGGESTED_FEE_RECIPIENT_ADDRESS, null, null, null, "0x1c9c380");
    assertThat(parameter.serialize()).contains("\"targetGasLimit\":30000000");
  }

  @Test
  public void serialize_TargetGasLimitOmittedWhenNull() {
    assertThat(parameterWithdrawalsOmitted().serialize()).doesNotContain("targetGasLimit");
  }

  private EnginePayloadAttributesParameter parameterWithdrawalsOmitted() {
    return new EnginePayloadAttributesParameter(
        TIMESTAMP, PREV_RANDAO, SUGGESTED_FEE_RECIPIENT_ADDRESS, null, null, null, null);
  }

  private EnginePayloadAttributesParameter parameterWithdrawalsPresent() {
    final List<WithdrawalParameter> withdrawals = List.of(WITHDRAWAL_PARAM_1, WITHDRAWAL_PARAM_2);
    return new EnginePayloadAttributesParameter(
        TIMESTAMP, PREV_RANDAO, SUGGESTED_FEE_RECIPIENT_ADDRESS, withdrawals, null, null, null);
  }

  // TODO: add a parent beacon block root test here
}
