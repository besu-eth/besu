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

import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.ethereum.core.ChainId;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"chainId", "address", "nonce", "yParity", "r", "s"})
public class CodeDelegationResult {
  private final String chainId;
  private final String address;
  private final String nonce;
  private final String yParity;
  private final String r;
  private final String s;

  public CodeDelegationResult(final CodeDelegation codeDelegation) {
    chainId = ChainId.fromCodeDelegation(codeDelegation).toHexString();
    address = codeDelegation.address().getBytes().toHexString();
    nonce = Quantity.create(codeDelegation.nonce());
    yParity = Quantity.create(codeDelegation.v());
    r = Quantity.create(codeDelegation.r());
    s = Quantity.create(codeDelegation.s());
  }

  @JsonProperty("chainId")
  public String chainId() {
    return chainId;
  }

  @JsonProperty("address")
  public String address() {
    return address;
  }

  @JsonProperty("nonce")
  public String nonce() {
    return nonce;
  }

  @JsonProperty("yParity")
  public String yParity() {
    return yParity;
  }

  @JsonProperty("r")
  public String r() {
    return r;
  }

  @JsonProperty("s")
  public String s() {
    return s;
  }
}
