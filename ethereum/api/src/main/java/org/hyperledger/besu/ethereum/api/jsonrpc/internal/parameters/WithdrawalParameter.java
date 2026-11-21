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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcObjectMapperFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.Withdrawal;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.core.json.JsonObject;
import org.apache.tuweni.units.bigints.UInt64;

public class WithdrawalParameter {

  private final String index;
  private final String validatorIndex;
  private final String address;
  private final String amount;

  @JsonCreator
  public WithdrawalParameter(
      @JsonProperty("index") final String index,
      @JsonProperty("validatorIndex") final String validatorIndex,
      @JsonProperty("address") final String address,
      @JsonProperty("amount") final String amount) {
    this.index = index;
    this.validatorIndex = validatorIndex;
    this.address = address;
    this.amount = amount;
  }

  public static WithdrawalParameter fromWithdrawal(final Withdrawal withdrawal) {
    return new WithdrawalParameter(
        withdrawal.getIndex().toBytes().toQuantityHexString(),
        withdrawal.getValidatorIndex().toBytes().toQuantityHexString(),
        withdrawal.getAddress().toString(),
        withdrawal.getAmount().toShortHexString());
  }

  /**
   * Converts this parameter to a withdrawal.
   *
   * @return the withdrawal
   * @throws InvalidJsonRpcParameters if the address is not {@code 0x} or {@code 0X} followed by
   *     exactly 40 hex digits, so a truncated address is rejected instead of being left-padded to a
   *     different account
   */
  public Withdrawal toWithdrawal() {
    final Address withdrawalAddress =
        JsonRpcObjectMapperFactory.parseStrictAddress(address)
            .orElseThrow(
                () ->
                    new InvalidJsonRpcParameters(
                        "Invalid withdrawal address: "
                            + JsonRpcObjectMapperFactory.STRICT_ADDRESS_MESSAGE,
                        RpcErrorType.INVALID_PARAMS));
    return new Withdrawal(
        UInt64.fromHexString(index),
        UInt64.fromHexString(validatorIndex),
        withdrawalAddress,
        GWei.fromHexString(amount));
  }

  public JsonObject asJsonObject() {
    return new JsonObject()
        .put("index", index)
        .put("validatorIndex", validatorIndex)
        .put("address", address)
        .put("amount", amount);
  }

  @JsonGetter
  public String getIndex() {
    return index;
  }

  @JsonGetter
  public String getValidatorIndex() {
    return validatorIndex;
  }

  @JsonGetter
  public String getAddress() {
    return address;
  }

  @JsonGetter
  public String getAmount() {
    return amount;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final WithdrawalParameter that = (WithdrawalParameter) o;
    return Objects.equals(index, that.index)
        && Objects.equals(validatorIndex, that.validatorIndex)
        && Objects.equals(address, that.address)
        && Objects.equals(amount, that.amount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, validatorIndex, address, amount);
  }

  @Override
  public String toString() {
    return "WithdrawalParameter{"
        + "index='"
        + index
        + '\''
        + ", validatorIndex='"
        + validatorIndex
        + '\''
        + ", address='"
        + address
        + '\''
        + ", amount='"
        + amount
        + '\''
        + '}';
  }
}
