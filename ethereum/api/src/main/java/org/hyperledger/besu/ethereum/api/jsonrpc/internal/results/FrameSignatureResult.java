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

import org.hyperledger.besu.ethereum.core.FrameSignature;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/** JSON-RPC representation of an EIP-8141 signature entry, matching geth. */
@JsonPropertyOrder({"scheme", "signer", "msg", "signature"})
public record FrameSignatureResult(
    @JsonProperty("scheme") String scheme,
    @JsonProperty("signer") String signer,
    @JsonProperty("msg") String msg,
    @JsonProperty("signature") String signature) {

  /**
   * Renders a signature entry.
   *
   * @param signature the signature entry
   * @return its JSON-RPC representation
   */
  public static FrameSignatureResult of(final FrameSignature signature) {
    return new FrameSignatureResult(
        Quantity.create(signature.scheme()),
        signature.signer().toHexString(),
        signature.msg().toHexString(),
        signature.signature().toHexString());
  }
}
