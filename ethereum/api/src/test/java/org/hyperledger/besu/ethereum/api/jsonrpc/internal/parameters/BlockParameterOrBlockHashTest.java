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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;

public class BlockParameterOrBlockHashTest {

  @Test
  public void hexBlockNumberStringIsAccepted() throws JsonProcessingException {
    final BlockParameterOrBlockHash param = new BlockParameterOrBlockHash("0x64");
    assertThat(param.getNumber()).hasValue(100);
  }

  @Test
  public void decimalBlockNumberStringIsRejected() {
    assertThatThrownBy(() -> new BlockParameterOrBlockHash("100"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("0x prefix");
  }

  @Test
  public void hexBlockNumberInEip1898ObjectFormIsAccepted() throws JsonProcessingException {
    // EIP-1898 object form: {"blockNumber": "0x64"}
    final BlockParameterOrBlockHash param =
        new BlockParameterOrBlockHash(java.util.Map.of("blockNumber", "0x64"));
    assertThat(param.getNumber()).hasValue(100);
  }

  @Test
  public void decimalBlockNumberInEip1898ObjectFormIsRejected() {
    // EIP-1898 object form with decimal: {"blockNumber": "100"} must be rejected
    assertThatThrownBy(() -> new BlockParameterOrBlockHash(java.util.Map.of("blockNumber", "100")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("0x prefix");
  }
}
