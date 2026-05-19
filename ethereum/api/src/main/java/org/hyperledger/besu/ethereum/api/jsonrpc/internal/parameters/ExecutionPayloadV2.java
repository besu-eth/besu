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

import org.hyperledger.besu.ethereum.core.Withdrawal;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonSetter;

public sealed class ExecutionPayloadV2 extends ExecutionPayloadV1 permits ExecutionPayloadV3 {
  private List<Withdrawal> withdrawals;

  @JsonSetter("withdrawals")
  public void setWithdrawals(final List<Withdrawal> withdrawals) {
    this.withdrawals = withdrawals;
  }

  public List<Withdrawal> getWithdrawals() {
    return withdrawals;
  }
}
