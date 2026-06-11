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
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ExecutionPayloadBodiesV2;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EngineGetPayloadBodiesByRangeV2Test extends EngineGetPayloadBodiesByRangeV1Test {

  @Override
  protected EngineGetPayloadBodiesByRangeV1<?> createMethodInstance(final int maxRequestBlocks) {
    return new EngineGetPayloadBodiesByRangeV2<>(
        protocolSchedule, protocolContext, vertx, engineCallListener, maxRequestBlocks);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getPayloadBodiesByRangeV2");
  }

  @Override
  @Test
  public void shouldReturnNullForWithdrawalsWhenBlockIsPreShanghai() {
    // not applicable to V2
  }

  @Test
  public void shouldReturnBlockAccessListWhenAvailable() {
    final SignatureAlgorithm sig = SignatureAlgorithmFactory.getInstance();
    final BlockAccessList blockAccessList = createSampleBlockAccessList();

    final Block block1 =
        blockWithBody(
            1L,
            new BlockBody(
                List.of(new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
                Collections.emptyList()));
    final Block block2 =
        blockWithBody(
            2L,
            new BlockBody(
                List.of(new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
                Collections.emptyList()));

    when(blockchain.getChainHeadBlockNumber()).thenReturn(2L);
    when(blockchain.getBlockByNumber(1L)).thenReturn(Optional.of(block1));
    when(blockchain.getBlockByNumber(2L)).thenReturn(Optional.of(block2));
    when(blockchain.getBlockAccessList(block1.getHash())).thenReturn(Optional.of(blockAccessList));
    when(blockchain.getBlockAccessList(block2.getHash())).thenReturn(Optional.empty());

    final List<ExecutionPayloadBodiesV2> result = fromSuccessRespV2(resp("0x1", "0x2"));
    assertThat(result.size()).isEqualTo(2);
    assertThat(result.get(0).getBlockAccessList()).isEqualTo(blockAccessList);
    assertThat(result.get(1).getBlockAccessList()).isNull();
  }

  @Test
  public void shouldReturnNullBlockAccessListForPreAmsterdamBlock() {
    final SignatureAlgorithm sig = SignatureAlgorithmFactory.getInstance();
    final Block block =
        blockWithBody(
            1L,
            new BlockBody(
                List.of(new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
                Collections.emptyList()));

    when(blockchain.getChainHeadBlockNumber()).thenReturn(1L);
    when(blockchain.getBlockByNumber(1L)).thenReturn(Optional.of(block));
    // blockchain.getBlockAccessList returns Optional.empty() by default

    final List<ExecutionPayloadBodiesV2> result = fromSuccessRespV2(resp("0x1", "0x1"));
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0).getBlockAccessList()).isNull();
  }

  private JsonRpcResponse resp(final String startBlockNumber, final String range) {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0",
                RpcMethod.ENGINE_GET_PAYLOAD_BODIES_BY_RANGE_V2.getMethodName(),
                new Object[] {startBlockNumber, range})));
  }

  @SuppressWarnings("unchecked")
  private List<ExecutionPayloadBodiesV2> fromSuccessRespV2(final JsonRpcResponse resp) {
    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    return (List<ExecutionPayloadBodiesV2>) ((JsonRpcSuccessResponse) resp).getResult();
  }

  private static Block blockWithBody(final long blockNumber, final BlockBody body) {
    return new Block(new BlockHeaderTestFixture().number(blockNumber).buildHeader(), body);
  }

  private static BlockAccessList createSampleBlockAccessList() {
    final Address address = Address.fromHexString("0x0000000000000000000000000000000000000001");
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.ONE);
    final BlockAccessList.SlotChanges slotChanges =
        new BlockAccessList.SlotChanges(
            slotKey, List.of(new BlockAccessList.StorageChange(0, UInt256.valueOf(2))));
    return new BlockAccessList(
        List.of(
            new BlockAccessList.AccountChanges(
                address,
                List.of(slotChanges),
                List.of(new BlockAccessList.SlotRead(slotKey)),
                List.of(new BlockAccessList.BalanceChange(0, Wei.ONE)),
                List.of(),
                List.of())));
  }
}
