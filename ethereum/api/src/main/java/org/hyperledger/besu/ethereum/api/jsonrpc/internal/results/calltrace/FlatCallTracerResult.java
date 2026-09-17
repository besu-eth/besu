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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Represents the result format for Ethereum's flatCallTracer as specified in Geth.
 *
 * <p>Produces a flattened list of call frames matching the parity-style trace format.
 */
@JsonPropertyOrder({
  "action",
  "blockHash",
  "blockNumber",
  "error",
  "result",
  "subtraces",
  "traceAddress",
  "transactionHash",
  "transactionPosition",
  "type"
})
public record FlatCallTracerResult(
    Action action,
    String blockHash,
    long blockNumber,
    @JsonInclude(JsonInclude.Include.NON_NULL) String error,
    @JsonInclude(JsonInclude.Include.NON_NULL) Result result,
    int subtraces,
    List<Integer> traceAddress,
    String transactionHash,
    int transactionPosition,
    String type) {

  /** Action details for a flat call trace frame. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder({
    "address",
    "balance",
    "callType",
    "creationMethod",
    "from",
    "gas",
    "init",
    "input",
    "refundAddress",
    "to",
    "value"
  })
  public record Action(
      String address,
      String balance,
      String callType,
      String creationMethod,
      String from,
      String gas,
      String init,
      String input,
      String refundAddress,
      String to,
      String value) {}

  /** Result details for a flat call trace frame. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder({"address", "code", "gasUsed", "output"})
  public record Result(String address, String code, String gasUsed, String output) {}
}
