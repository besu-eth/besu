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

import org.hyperledger.besu.ethereum.core.Frame;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * JSON-RPC representation of an EIP-8141 frame, matching geth: the target is omitted when the frame
 * targets the sender.
 */
@JsonPropertyOrder({"mode", "flags", "target", "executionGas", "stateGas", "value", "data"})
public record FrameResult(
    @JsonProperty("mode") String mode,
    @JsonProperty("flags") String flags,
    @JsonProperty("target") @JsonInclude(JsonInclude.Include.NON_NULL) String target,
    @JsonProperty("executionGas") String executionGas,
    @JsonProperty("stateGas") String stateGas,
    @JsonProperty("value") String value,
    @JsonProperty("data") String data) {

  /**
   * Renders a frame.
   *
   * @param frame the frame
   * @return its JSON-RPC representation
   */
  public static FrameResult of(final Frame frame) {
    return new FrameResult(
        Quantity.create(frame.mode()),
        Quantity.create(frame.flags()),
        frame.target().map(Object::toString).orElse(null),
        Quantity.create(frame.executionGasLimit()),
        Quantity.create(frame.stateGasLimit()),
        Quantity.create(frame.value()),
        frame.data().toHexString());
  }
}
