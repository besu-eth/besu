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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.tuweni.bytes.Bytes;

@JsonPropertyOrder(alphabetic = true)
public class CallProcessingResult {
  @JsonProperty("status")
  private final String status;

  @JsonProperty("returnData")
  private final String returnData;

  @JsonProperty("gasUsed")
  private final String gasUsed;

  @JsonProperty("maxUsedGas")
  private final String maxUsedGas;

  @JsonProperty("error")
  private final JsonRpcError error;

  @JsonProperty("logs")
  private final LogsResult logs;

  public CallProcessingResult(
      @JsonProperty("status") final int status,
      @JsonProperty("returnData") final Bytes returnData,
      @JsonProperty("gasUsed") final long gasUsed,
      @JsonProperty("maxUsedGas") final long maxUsedGas,
      @JsonProperty("error") final JsonRpcError error,
      @JsonProperty("logs") final LogsResult logs) {
    this.status = Quantity.create(status);
    this.returnData = returnData.toString();
    this.gasUsed = Quantity.create(gasUsed);
    this.maxUsedGas = Quantity.create(maxUsedGas);
    this.error = error;
    this.logs = logs;
  }

  public String getStatus() {
    return status;
  }

  public String getReturnData() {
    return returnData;
  }

  public String getGasUsed() {
    return gasUsed;
  }

  public String getMaxUsedGas() {
    return maxUsedGas;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public JsonRpcError getError() {
    return error;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public LogsResult getLogs() {
    return logs;
  }
}
