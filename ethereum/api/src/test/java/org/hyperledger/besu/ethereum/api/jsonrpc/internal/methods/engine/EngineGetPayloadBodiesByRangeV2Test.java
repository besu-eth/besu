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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineTestSupport.fromErrorResp;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_PARAMS;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_RANGE_REQUEST_TOO_LARGE;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetPayloadBodiesResultV2;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EngineGetPayloadBodiesByRangeV2Test extends AbstractEngineGetPayloadBodiesTest {

  public EngineGetPayloadBodiesByRangeV2Test() {
    super(EngineGetPayloadBodiesByRangeV2::new);
  }

  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getPayloadBodiesByRangeV2");
  }

  @Test
  public void shouldReturnBlockAccessListWhenAvailable() {
    final SignatureAlgorithm sig = SignatureAlgorithmFactory.getInstance();
    final Hash blockHash1 = Hash.wrap(Bytes32.random());
    final Hash blockHash2 = Hash.wrap(Bytes32.random());
    final BlockBody blockBody1 =
        new BlockBody(
            List.of(new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
            Collections.emptyList());
    final BlockBody blockBody2 =
        new BlockBody(
            List.of(new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
            Collections.emptyList());

    final BlockAccessList blockAccessList = createSampleBlockAccessList();
    final String encodedBlockAccessList = encodeBlockAccessList(blockAccessList);

    when(blockchain.getChainHeadBlockNumber()).thenReturn(2L);
    when(blockchain.getBlockHashByNumber(1L)).thenReturn(Optional.of(blockHash1));
    when(blockchain.getBlockHashByNumber(2L)).thenReturn(Optional.of(blockHash2));
    when(blockchain.getBlockBody(blockHash1)).thenReturn(Optional.of(blockBody1));
    when(blockchain.getBlockBody(blockHash2)).thenReturn(Optional.of(blockBody2));
    when(blockchain.getBlockAccessList(blockHash1)).thenReturn(Optional.of(blockAccessList));
    when(blockchain.getBlockAccessList(blockHash2)).thenReturn(Optional.empty());

    final var resp = resp("0x1", "0x2");
    final EngineGetPayloadBodiesResultV2 result = fromSuccessResp(resp);
    assertThat(result.getPayloadBodies().size()).isEqualTo(2);
    assertThat(result.getPayloadBodies().get(0).getBlockAccessList())
        .isEqualTo(encodedBlockAccessList);
    assertThat(result.getPayloadBodies().get(1).getBlockAccessList()).isNull();
  }

  @Test
  public void shouldReturnNullForUnknownNumber() {
    when(blockchain.getChainHeadBlockNumber()).thenReturn(Long.valueOf(130));
    final var resp = resp("0x7b", "0x3");
    final EngineGetPayloadBodiesResultV2 result = fromSuccessResp(resp);
    assertThat(result.getPayloadBodies().size()).isEqualTo(3);
    assertThat(result.getPayloadBodies().get(0)).isNull();
    assertThat(result.getPayloadBodies().get(1)).isNull();
    assertThat(result.getPayloadBodies().get(2)).isNull();
  }

  @Test
  public void shouldNotContainTrailingNullForBlocksPastTheCurrentHead() {
    final SignatureAlgorithm sig = SignatureAlgorithmFactory.getInstance();
    final Hash blockHash1 = Hash.wrap(Bytes32.random());

    final BlockBody blockBody =
        new BlockBody(
            List.of(
                new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
            Collections.emptyList(),
            Optional.of(Collections.emptyList()));

    when(blockchain.getChainHeadBlockNumber()).thenReturn(Long.valueOf(123));
    when(blockchain.getBlockBody(blockHash1)).thenReturn(Optional.of(blockBody));
    when(blockchain.getBlockHashByNumber(123)).thenReturn(Optional.of(blockHash1));

    final var resp = resp("0x7b", "0x3");
    final var result = fromSuccessResp(resp);
    assertThat(result.getPayloadBodies().size()).isEqualTo(1);
  }

  @Test
  public void shouldReturnEmptyPayloadForRequestsPastCurrentHead() {

    when(blockchain.getChainHeadBlockNumber()).thenReturn(Long.valueOf(123));
    final JsonRpcResponse resp = resp("0x7d", "0x3");
    final var result = fromSuccessResp(resp);
    assertThat(result.getPayloadBodies()).isEqualTo(Collections.EMPTY_LIST);
  }

  @Test
  public void shouldReturnErrorWhenRequestExceedsPermittedNumberOfBlocks() {
    doReturn(3).when(method).getMaxRequestBlocks();
    final JsonRpcResponse resp = resp("0x539", "0x4");
    final var result = fromErrorResp(resp);
    assertThat(result.getCode()).isEqualTo(INVALID_RANGE_REQUEST_TOO_LARGE.getCode());
  }

  @Test
  public void shouldReturnInvalidParamsIfStartIsZero() {
    final JsonRpcResponse resp = resp("0x0", "0x539");
    final var result = fromErrorResp(resp);
    assertThat(result.getCode()).isEqualTo(INVALID_PARAMS.getCode());
  }

  @Test
  public void shouldReturnInvalidParamsIfCountIsZero() {
    final JsonRpcResponse resp = resp("0x539", "0x0");
    final var result = fromErrorResp(resp);
    assertThat(result.getCode()).isEqualTo(INVALID_PARAMS.getCode());
  }

  private JsonRpcResponse resp(final String startBlockNumber, final String range) {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0",
                RpcMethod.ENGINE_GET_PAYLOAD_BODIES_BY_RANGE_V2.getMethodName(),
                new Object[] {startBlockNumber, range})));
  }
}
