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
package org.hyperledger.besu.ethereum.api.pluginadapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.plugin.services.exception.InProcessRpcDisabledException;
import org.hyperledger.besu.plugin.services.rpc.PluginRpcResponse;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

class InProcessRpcServiceImplTest {

  @Test
  void disabledIsReportedDistinctlyFromAnUnknownMethod() {
    final InProcessRpcServiceImpl disabled = new InProcessRpcServiceImpl(false, Map.of());

    assertThat(disabled.isEnabled()).isFalse();
    assertThatThrownBy(() -> disabled.call("eth_blockNumber", new Object[0]))
        .isInstanceOf(InProcessRpcDisabledException.class)
        .hasMessageContaining("--Xin-process-rpc-enabled");
  }

  @Test
  void unknownMethodIsReportedWhenEnabled() {
    final InProcessRpcServiceImpl enabled = new InProcessRpcServiceImpl(true, Map.of());

    assertThat(enabled.isEnabled()).isTrue();
    assertThatThrownBy(() -> enabled.call("eth_blockNumber", new Object[0]))
        .isInstanceOf(NoSuchElementException.class)
        .hasMessageContaining("eth_blockNumber");
  }

  @Test
  void enabledMethodIsCalledAndItsResultReturned() {
    final JsonRpcMethod method = mock(JsonRpcMethod.class);
    when(method.response(any())).thenReturn(new JsonRpcSuccessResponse(1, "0x10"));
    final InProcessRpcServiceImpl enabled =
        new InProcessRpcServiceImpl(true, Map.of("eth_blockNumber", method));

    final PluginRpcResponse response = enabled.call("eth_blockNumber", new Object[0]);

    assertThat(response.getType()).isEqualTo(RpcResponseType.SUCCESS);
    assertThat(response.getResult()).isEqualTo("0x10");
  }
}
