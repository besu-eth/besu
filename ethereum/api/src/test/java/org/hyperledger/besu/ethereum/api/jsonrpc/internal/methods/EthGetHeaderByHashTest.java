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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockHeaderResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EthGetHeaderByHashTest {

  @Mock private BlockchainQueries blockchainQueries;
  private EthGetHeaderByHash method;
  private final String JSON_RPC_VERSION = "2.0";
  private final String ETH_METHOD = "eth_getHeaderByHash";
  private final String ZERO_HASH = String.valueOf(Hash.ZERO);

  @BeforeEach
  public void setUp() {
    method = new EthGetHeaderByHash(blockchainQueries);
  }

  @Test
  public void returnsCorrectMethodName() {
    assertThat(method.getName()).isEqualTo(ETH_METHOD);
  }

  @Test
  public void exceptionWhenNoParamsSupplied() {
    assertThatThrownBy(() -> method.response(requestWithParams()))
        .isInstanceOf(InvalidJsonRpcParameters.class);
    verifyNoMoreInteractions(blockchainQueries);
  }

  @Test
  public void exceptionWhenHashParamInvalid() {
    assertThatThrownBy(() -> method.response(requestWithParams("hash")))
        .isInstanceOf(InvalidJsonRpcParameters.class)
        .hasMessage("Invalid block hash parameter (index 0)");
    verifyNoMoreInteractions(blockchainQueries);
  }

  @Test
  public void nullWhenHashUnknown() {
    when(blockchainQueries.getBlockHeaderByHash(Hash.ZERO)).thenReturn(Optional.empty());

    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) method.response(requestWithParams(ZERO_HASH));
    assertThat(response.getResult()).isNull();
  }

  @Test
  public void returnsHeaderWhenHashKnown() {
    final BlockHeader header = new BlockHeaderTestFixture().number(7).buildHeader();
    when(blockchainQueries.getBlockHeaderByHash(header.getHash())).thenReturn(Optional.of(header));

    final JsonRpcSuccessResponse response =
        (JsonRpcSuccessResponse) method.response(requestWithParams(header.getHash().toString()));
    final BlockHeaderResult result = (BlockHeaderResult) response.getResult();
    assertThat(result.getHash()).isEqualTo(header.getHash().toString());
    assertThat(result.getNumber()).isEqualTo("0x7");
  }

  private JsonRpcRequestContext requestWithParams(final Object... params) {
    return new JsonRpcRequestContext(new JsonRpcRequest(JSON_RPC_VERSION, ETH_METHOD, params));
  }
}
