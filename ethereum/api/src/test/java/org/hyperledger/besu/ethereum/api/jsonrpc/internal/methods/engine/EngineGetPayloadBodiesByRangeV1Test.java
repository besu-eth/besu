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
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_PARAMS;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_RANGE_REQUEST_TOO_LARGE;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ExecutionPayloadBodiesV1;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;
import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EngineGetPayloadBodiesByRangeV1Test extends AbstractScheduledApiTest {
  protected EngineGetPayloadBodiesByRangeV1<?> method;
  protected static final Vertx vertx = Vertx.vertx();
  @Mock protected ProtocolContext protocolContext;
  @Mock protected EngineCallListener engineCallListener;
  @Mock protected MutableBlockchain blockchain;

  @Override
  @BeforeEach
  public void before() {
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    this.method = createMethodInstance(10);
  }

  protected EngineGetPayloadBodiesByRangeV1<?> createMethodInstance(final int maxRequestBlocks) {
    return new EngineGetPayloadBodiesByRangeV1<>(
        protocolSchedule, protocolContext, vertx, engineCallListener, maxRequestBlocks);
  }

  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getPayloadBodiesByRangeV1");
  }

  @Test
  public void shouldReturnPayloadForKnownNumber() {
    final SignatureAlgorithm sig = SignatureAlgorithmFactory.getInstance();
    final Block block1 =
        blockWithBody(
            new BlockBody(
                List.of(new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
                Collections.emptyList()));
    final Block block2 =
        blockWithBody(
            new BlockBody(
                List.of(
                    new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                    new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
                Collections.emptyList()));
    final Block block3 =
        blockWithBody(
            new BlockBody(
                List.of(
                    new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                    new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                    new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
                Collections.emptyList()));
    when(blockchain.getChainHeadBlockNumber()).thenReturn(130L);
    when(blockchain.getBlockByNumber(123)).thenReturn(Optional.of(block1));
    when(blockchain.getBlockByNumber(124)).thenReturn(Optional.of(block2));
    when(blockchain.getBlockByNumber(125)).thenReturn(Optional.of(block3));

    final List<ExecutionPayloadBodiesV1> result = fromSuccessResp(resp("0x7b", "0x3"));
    assertThat(result.size()).isEqualTo(3);
    assertThat(result.get(0).getTransactions().size()).isEqualTo(1);
    assertThat(result.get(1).getTransactions().size()).isEqualTo(2);
    assertThat(result.get(2).getTransactions().size()).isEqualTo(3);
  }

  @Test
  public void shouldReturnNullForUnknownNumber() {
    when(blockchain.getChainHeadBlockNumber()).thenReturn(130L);
    // blockchain.getBlockByNumber returns Optional.empty() by default

    final List<ExecutionPayloadBodiesV1> result = fromSuccessResp(resp("0x7b", "0x3"));
    assertThat(result.size()).isEqualTo(3);
    assertThat(result.get(0)).isNull();
    assertThat(result.get(1)).isNull();
    assertThat(result.get(2)).isNull();
  }

  @Test
  public void shouldReturnNullForUnknownNumberAndPayloadForKnownNumber() {
    final SignatureAlgorithm sig = SignatureAlgorithmFactory.getInstance();
    final Block block1 =
        blockWithBody(
            new BlockBody(
                List.of(new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
                Collections.emptyList()));
    final Block block3 =
        blockWithBody(
            new BlockBody(
                List.of(
                    new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                    new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                    new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
                Collections.emptyList()));
    when(blockchain.getChainHeadBlockNumber()).thenReturn(130L);
    when(blockchain.getBlockByNumber(123)).thenReturn(Optional.of(block1));
    when(blockchain.getBlockByNumber(125)).thenReturn(Optional.of(block3));

    final List<ExecutionPayloadBodiesV1> result = fromSuccessResp(resp("0x7b", "0x3"));
    assertThat(result.size()).isEqualTo(3);
    assertThat(result.get(0).getTransactions().size()).isEqualTo(1);
    assertThat(result.get(1)).isNull();
    assertThat(result.get(2).getTransactions().size()).isEqualTo(3);
  }

  @Test
  public void shouldReturnNullForWithdrawalsWhenBlockIsPreShanghai() {
    final SignatureAlgorithm sig = SignatureAlgorithmFactory.getInstance();
    final Block block1 =
        blockWithBody(
            new BlockBody(
                List.of(
                    new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                    new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                    new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
                Collections.emptyList()));
    final Block block2 =
        blockWithBody(
            new BlockBody(
                List.of(new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
                Collections.emptyList(),
                Optional.empty()));
    when(blockchain.getChainHeadBlockNumber()).thenReturn(130L);
    when(blockchain.getBlockByNumber(123)).thenReturn(Optional.of(block1));
    when(blockchain.getBlockByNumber(124)).thenReturn(Optional.of(block2));

    final List<ExecutionPayloadBodiesV1> result = fromSuccessResp(resp("0x7b", "0x2"));
    assertThat(result.size()).isEqualTo(2);
    assertThat(result.get(0).getTransactions().size()).isEqualTo(3);
    assertThat(result.get(0).getWithdrawals()).isNull();
    assertThat(result.get(1).getTransactions().size()).isEqualTo(1);
    assertThat(result.get(1).getWithdrawals()).isNull();
  }

  @Test
  public void shouldReturnWithdrawalsWhenBlockIsPostShanghai() {
    final SignatureAlgorithm sig = SignatureAlgorithmFactory.getInstance();
    final Withdrawal withdrawal =
        new Withdrawal(UInt64.ONE, UInt64.ONE, Address.fromHexString("0x1"), GWei.ONE);
    final Withdrawal withdrawal2 =
        new Withdrawal(UInt64.ONE, UInt64.ONE, Address.fromHexString("0x2"), GWei.ONE);
    final Block block1 =
        blockWithBody(
            new BlockBody(
                List.of(
                    new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                    new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                    new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
                Collections.emptyList(),
                Optional.of(List.of(withdrawal))));
    final Block block2 =
        blockWithBody(
            new BlockBody(
                List.of(new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
                Collections.emptyList(),
                Optional.of(List.of(withdrawal2))));
    when(blockchain.getChainHeadBlockNumber()).thenReturn(130L);
    when(blockchain.getBlockByNumber(123)).thenReturn(Optional.of(block1));
    when(blockchain.getBlockByNumber(124)).thenReturn(Optional.of(block2));

    final List<ExecutionPayloadBodiesV1> result = fromSuccessResp(resp("0x7b", "0x2"));
    assertThat(result.size()).isEqualTo(2);
    assertThat(result.get(0).getTransactions().size()).isEqualTo(3);
    assertThat(result.get(0).getWithdrawals().size()).isEqualTo(1);
    assertThat(result.get(1).getTransactions().size()).isEqualTo(1);
    assertThat(result.get(1).getWithdrawals().size()).isEqualTo(1);
  }

  @Test
  public void shouldNotContainTrailingNullForBlocksPastTheCurrentHead() {
    final SignatureAlgorithm sig = SignatureAlgorithmFactory.getInstance();
    final Withdrawal withdrawal =
        new Withdrawal(UInt64.ONE, UInt64.ONE, Address.fromHexString("0x1"), GWei.ONE);
    final Block block1 =
        blockWithBody(
            new BlockBody(
                List.of(
                    new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                    new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                    new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
                Collections.emptyList(),
                Optional.of(List.of(withdrawal))));
    when(blockchain.getChainHeadBlockNumber()).thenReturn(123L);
    when(blockchain.getBlockByNumber(123)).thenReturn(Optional.of(block1));

    final List<ExecutionPayloadBodiesV1> result = fromSuccessResp(resp("0x7b", "0x3"));
    assertThat(result.size()).isEqualTo(1);
  }

  @Test
  public void shouldReturnUpUntilHeadWhenStartBlockPlusCountEqualsHeadNumber() {
    final SignatureAlgorithm sig = SignatureAlgorithmFactory.getInstance();
    final Withdrawal withdrawal =
        new Withdrawal(UInt64.ONE, UInt64.ONE, Address.fromHexString("0x1"), GWei.ONE);
    final Block block =
        blockWithBody(
            new BlockBody(
                List.of(new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
                Collections.emptyList(),
                Optional.of(List.of(withdrawal))));
    when(blockchain.getChainHeadBlockNumber()).thenReturn(125L);
    when(blockchain.getBlockByNumber(123)).thenReturn(Optional.of(block));
    when(blockchain.getBlockByNumber(124)).thenReturn(Optional.of(block));
    when(blockchain.getBlockByNumber(125)).thenReturn(Optional.of(block));

    final List<ExecutionPayloadBodiesV1> result = fromSuccessResp(resp("0x7b", "0x3"));
    assertThat(result.size()).isEqualTo(3);
  }

  @Test
  public void ShouldReturnEmptyPayloadForRequestsPastCurrentHead() {
    when(blockchain.getChainHeadBlockNumber()).thenReturn(123L);
    final List<ExecutionPayloadBodiesV1> result = fromSuccessResp(resp("0x7d", "0x3"));
    assertThat(result).isEqualTo(Collections.EMPTY_LIST);
  }

  @Test
  public void shouldReturnErrorWhenRequestExceedsPermittedNumberOfBlocks() {
    this.method = createMethodInstance(3);
    assertThat(fromErrorResp(resp("0x539", "0x4")).getCode())
        .isEqualTo(INVALID_RANGE_REQUEST_TOO_LARGE.getCode());
  }

  @Test
  public void shouldReturnInvalidParamsIfStartIsZero() {
    assertThat(fromErrorResp(resp("0x0", "0x539")).getCode()).isEqualTo(INVALID_PARAMS.getCode());
  }

  @Test
  public void shouldReturnInvalidParamsIfCountIsZero() {
    assertThat(fromErrorResp(resp("0x539", "0x0")).getCode()).isEqualTo(INVALID_PARAMS.getCode());
  }

  private static Block blockWithBody(final BlockBody body) {
    return new Block(new BlockHeaderTestFixture().buildHeader(), body);
  }

  private JsonRpcResponse resp(final String startBlockNumber, final String range) {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0",
                RpcMethod.ENGINE_GET_PAYLOAD_BODIES_BY_RANGE_V1.getMethodName(),
                new Object[] {startBlockNumber, range})));
  }

  @SuppressWarnings("unchecked")
  private List<ExecutionPayloadBodiesV1> fromSuccessResp(final JsonRpcResponse resp) {
    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    return (List<ExecutionPayloadBodiesV1>) ((JsonRpcSuccessResponse) resp).getResult();
  }

  private JsonRpcError fromErrorResp(final JsonRpcResponse resp) {
    assertThat(resp.getType()).isEqualTo(RpcResponseType.ERROR);
    return Optional.of(resp)
        .map(JsonRpcErrorResponse.class::cast)
        .map(JsonRpcErrorResponse::getError)
        .get();
  }
}
