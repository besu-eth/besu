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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;

import org.junit.jupiter.api.Test;

class JsonRpcParameterTest {

  private final JsonRpcParameter parameterAccessor = new JsonRpcParameter();

  @Test
  void optional_throwsForBoolean_whenRawParamIsInteger() {
    assertThatExceptionOfType(JsonRpcParameterException.class)
        .isThrownBy(() -> parameterAccessor.optional(new Object[] {1}, 0, Boolean.class))
        .withMessageContaining("Invalid json rpc parameter at index 0");
  }

  @Test
  void optional_throwsForBoolean_whenRawParamIsStringTrue() {
    assertThatExceptionOfType(JsonRpcParameterException.class)
        .isThrownBy(() -> parameterAccessor.optional(new Object[] {"true"}, 0, Boolean.class))
        .withMessageContaining("Invalid json rpc parameter at index 0");
  }

  @Test
  void optional_throwsForBoolean_whenRawParamIsStringFalse() {
    assertThatExceptionOfType(JsonRpcParameterException.class)
        .isThrownBy(() -> parameterAccessor.optional(new Object[] {"false"}, 0, Boolean.class))
        .withMessageContaining("Invalid json rpc parameter at index 0");
  }

  @Test
  void optional_throwsForBoolean_whenRawParamIsEmptyString() {
    assertThatExceptionOfType(JsonRpcParameterException.class)
        .isThrownBy(() -> parameterAccessor.optional(new Object[] {""}, 0, Boolean.class))
        .withMessageContaining("Invalid json rpc parameter at index 0");
  }

  @Test
  void optional_acceptsBoolean_whenRawParamIsBooleanTrue() throws Exception {
    assertThat(parameterAccessor.optional(new Object[] {Boolean.TRUE}, 0, Boolean.class))
        .contains(Boolean.TRUE);
  }

  @Test
  void optional_acceptsBoolean_whenRawParamIsBooleanFalse() throws Exception {
    assertThat(parameterAccessor.optional(new Object[] {Boolean.FALSE}, 0, Boolean.class))
        .contains(Boolean.FALSE);
  }
}
