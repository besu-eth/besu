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
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_RANGE_REQUEST_TOO_LARGE;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConstructorArgumentsBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ExecutionPayloadBodiesV1;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes32;
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
public class EngineGetPayloadBodiesByHashV1Test extends AbstractScheduledApiTest {
  protected EngineGetPayloadBodiesByHashV1<?> method;
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

  protected EngineGetPayloadBodiesByHashV1<?> createMethodInstance(final int maxRequestBlocks) {
    return new EngineGetPayloadBodiesByHashV1<>(
        new ConstructorArgumentsBuilder()
            .protocolSchedule(protocolSchedule)
            .protocolContext(protocolContext)
            .vertx(vertx)
            .engineCallListener(engineCallListener)
            .maxRequestBlocks(maxRequestBlocks)
            .build(),
        null,
        null);
  }

  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getPayloadBodiesByHashV1");
  }

  @Test
  public void shouldReturnEmptyPayloadBodiesWithEmptyHash() {
    final var resp = resp(new Hash[] {});
    final List<ExecutionPayloadBodiesV1> result = fromSuccessResp(resp);
    assertThat(result.isEmpty()).isTrue();
  }

  @Test
  public void shouldReturnPayloadForKnownHashes() {
    final SignatureAlgorithm sig = SignatureAlgorithmFactory.getInstance();
    final Hash blockHash1 = Hash.wrap(Bytes32.random());
    final Hash blockHash2 = Hash.wrap(Bytes32.random());
    final Hash blockHash3 = Hash.wrap(Bytes32.random());
    final BlockBody blockBody1 =
        new BlockBody(
            List.of(new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
            Collections.emptyList());
    final BlockBody blockBody2 =
        new BlockBody(
            List.of(
                new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
            Collections.emptyList());
    final BlockBody blockBody3 =
        new BlockBody(
            List.of(
                new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
            Collections.emptyList());
    when(blockchain.getBlockBody(blockHash1)).thenReturn(Optional.of(blockBody1));
    when(blockchain.getBlockBody(blockHash2)).thenReturn(Optional.of(blockBody2));
    when(blockchain.getBlockBody(blockHash3)).thenReturn(Optional.of(blockBody3));

    final var resp = resp(new Hash[] {blockHash1, blockHash2, blockHash3});
    final List<ExecutionPayloadBodiesV1> result = fromSuccessResp(resp);
    assertThat(result.size()).isEqualTo(3);
    assertThat(result.get(0).getTransactions().size()).isEqualTo(1);
    assertThat(result.get(1).getTransactions().size()).isEqualTo(2);
    assertThat(result.get(2).getTransactions().size()).isEqualTo(3);
  }

  @Test
  public void shouldReturnNullForUnknownHashes() {
    final Hash blockHash1 = Hash.wrap(Bytes32.random());
    final Hash blockHash2 = Hash.wrap(Bytes32.random());
    final Hash blockHash3 = Hash.wrap(Bytes32.random());
    final var resp = resp(new Hash[] {blockHash1, blockHash2, blockHash3});
    final List<ExecutionPayloadBodiesV1> result = fromSuccessResp(resp);
    assertThat(result.size()).isEqualTo(3);
    assertThat(result.get(0)).isNull();
    assertThat(result.get(1)).isNull();
    assertThat(result.get(2)).isNull();
  }

  @Test
  public void shouldReturnNullForUnknownHashAndPayloadForKnownHash() {
    final SignatureAlgorithm sig = SignatureAlgorithmFactory.getInstance();
    final Hash blockHash1 = Hash.wrap(Bytes32.random());
    final Hash blockHash2 = Hash.wrap(Bytes32.random());
    final Hash blockHash3 = Hash.wrap(Bytes32.random());
    final BlockBody blockBody1 =
        new BlockBody(
            List.of(new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
            Collections.emptyList());
    final BlockBody blockBody3 =
        new BlockBody(
            List.of(
                new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
            Collections.emptyList());
    when(blockchain.getBlockBody(blockHash1)).thenReturn(Optional.of(blockBody1));
    when(blockchain.getBlockBody(blockHash3)).thenReturn(Optional.of(blockBody3));

    final var resp = resp(new Hash[] {blockHash1, blockHash2, blockHash3});
    final List<ExecutionPayloadBodiesV1> result = fromSuccessResp(resp);
    assertThat(result.size()).isEqualTo(3);
    assertThat(result.get(0).getTransactions().size()).isEqualTo(1);
    assertThat(result.get(1)).isNull();
    assertThat(result.get(2).getTransactions().size()).isEqualTo(3);
  }

  @Test
  public void shouldReturnWithdrawalNullWhenBlockIsPreShanghai() {
    final SignatureAlgorithm sig = SignatureAlgorithmFactory.getInstance();
    final Hash blockHash1 = Hash.wrap(Bytes32.random());
    final Hash blockHash2 = Hash.wrap(Bytes32.random());
    final BlockBody preShanghaiBlockBody =
        new BlockBody(
            List.of(
                new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
            Collections.emptyList());

    final BlockBody preShanghaiBlockBody2 =
        new BlockBody(
            List.of(new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
            Collections.emptyList(),
            Optional.empty());
    when(blockchain.getBlockBody(blockHash1)).thenReturn(Optional.of(preShanghaiBlockBody));
    when(blockchain.getBlockBody(blockHash2)).thenReturn(Optional.of(preShanghaiBlockBody2));

    final var resp = resp(new Hash[] {blockHash1, blockHash2});
    final List<ExecutionPayloadBodiesV1> result = fromSuccessResp(resp);
    assertThat(result.size()).isEqualTo(2);
    assertThat(result.get(0).getTransactions().size()).isEqualTo(3);
    assertThat(result.get(0).getWithdrawals()).isNull();
    assertThat(result.get(1).getTransactions().size()).isEqualTo(1);
    assertThat(result.get(1).getWithdrawals()).isNull();
  }

  @Test
  public void shouldReturnWithdrawalsWhenBlockIsPostShanghai() {
    final SignatureAlgorithm sig = SignatureAlgorithmFactory.getInstance();
    final Hash blockHash1 = Hash.wrap(Bytes32.random());
    final Hash blockHash2 = Hash.wrap(Bytes32.random());
    final Withdrawal withdrawal =
        new Withdrawal(UInt64.ONE, UInt64.ONE, Address.fromHexString("0x1"), GWei.ONE);
    final Withdrawal withdrawal2 =
        new Withdrawal(UInt64.ONE, UInt64.ONE, Address.fromHexString("0x2"), GWei.ONE);

    final BlockBody shanghaiBlockBody =
        new BlockBody(
            List.of(
                new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                new TransactionTestFixture().createTransaction(sig.generateKeyPair()),
                new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
            Collections.emptyList(),
            Optional.of(List.of(withdrawal)));

    final BlockBody shanghaiBlockBody2 =
        new BlockBody(
            List.of(new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
            Collections.emptyList(),
            Optional.of(List.of(withdrawal2)));
    when(blockchain.getBlockBody(blockHash1)).thenReturn(Optional.of(shanghaiBlockBody));
    when(blockchain.getBlockBody(blockHash2)).thenReturn(Optional.of(shanghaiBlockBody2));

    final var resp = resp(new Hash[] {blockHash1, blockHash2});
    final List<ExecutionPayloadBodiesV1> result = fromSuccessResp(resp);
    assertThat(result.size()).isEqualTo(2);
    assertThat(result.get(0).getTransactions().size()).isEqualTo(3);
    assertThat(result.get(0).getWithdrawals().size()).isEqualTo(1);
    assertThat(result.get(1).getTransactions().size()).isEqualTo(1);
    assertThat(result.get(1).getWithdrawals().size()).isEqualTo(1);
  }

  @Test
  public void shouldReturnErrorWhenRequestExceedsPermittedNumberOfBlocks() {
    this.method = createMethodInstance(1);

    final Hash blockHash1 = Hash.wrap(Bytes32.random());
    final Hash blockHash2 = Hash.wrap(Bytes32.random());
    final Hash[] hashes = new Hash[] {blockHash1, blockHash2};

    final JsonRpcResponse resp = resp(hashes);
    final var result = fromErrorResp(resp);
    assertThat(result.getCode()).isEqualTo(INVALID_RANGE_REQUEST_TOO_LARGE.getCode());
  }

  protected JsonRpcResponse resp(final Hash[] hashes) {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest("2.0", method.getName(), new Object[] {hashes})));
  }

  @SuppressWarnings("unchecked")
  protected List<ExecutionPayloadBodiesV1> fromSuccessResp(final JsonRpcResponse resp) {
    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    return (List<ExecutionPayloadBodiesV1>) ((JsonRpcSuccessResponse) resp).getResult();
  }
}
