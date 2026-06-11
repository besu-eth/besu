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
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ExecutionPayloadBodiesV2;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EngineGetPayloadBodiesByHashV2Test extends EngineGetPayloadBodiesByHashV1Test {

  @Override
  protected EngineGetPayloadBodiesByHashV1<?> createMethodInstance(final int maxRequestBlocks) {
    return new EngineGetPayloadBodiesByHashV2<>(
        protocolSchedule, protocolContext, vertx, engineCallListener, maxRequestBlocks);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getPayloadBodiesByHashV2");
  }

  @Override
  @Test
  public void shouldReturnWithdrawalNullWhenBlockIsPreShanghai() {
    // not applicable to V2
  }

  @Test
  public void shouldReturnBlockAccessListWhenAvailable() {
    final SignatureAlgorithm sig = SignatureAlgorithmFactory.getInstance();
    final Hash blockHash = Hash.wrap(Bytes32.random());
    final BlockBody blockBody =
        new BlockBody(
            List.of(new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
            Collections.emptyList());
    final BlockAccessList blockAccessList = createSampleBlockAccessList();

    when(blockchain.getBlockBody(blockHash)).thenReturn(Optional.of(blockBody));
    when(blockchain.getBlockAccessList(blockHash)).thenReturn(Optional.of(blockAccessList));

    final List<ExecutionPayloadBodiesV2> result = fromSuccessRespV2(resp(new Hash[] {blockHash}));
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0).getBlockAccessList()).isEqualTo(blockAccessList);
  }

  @Test
  public void shouldReturnNullBlockAccessListForPreAmsterdamBlock() {
    final SignatureAlgorithm sig = SignatureAlgorithmFactory.getInstance();
    final Hash blockHash = Hash.wrap(Bytes32.random());
    final BlockBody blockBody =
        new BlockBody(
            List.of(new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
            Collections.emptyList());

    when(blockchain.getBlockBody(blockHash)).thenReturn(Optional.of(blockBody));
    // blockchain.getBlockAccessList returns Optional.empty() by default

    final List<ExecutionPayloadBodiesV2> result = fromSuccessRespV2(resp(new Hash[] {blockHash}));
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0).getBlockAccessList()).isNull();
  }

  @Test
  public void shouldReturnNullBlockAccessListWhenPruned() {
    final SignatureAlgorithm sig = SignatureAlgorithmFactory.getInstance();
    final Hash blockHash = Hash.wrap(Bytes32.random());
    final BlockBody blockBody =
        new BlockBody(
            List.of(new TransactionTestFixture().createTransaction(sig.generateKeyPair())),
            Collections.emptyList());

    when(blockchain.getBlockBody(blockHash)).thenReturn(Optional.of(blockBody));
    when(blockchain.getBlockAccessList(blockHash)).thenReturn(Optional.empty());

    final List<ExecutionPayloadBodiesV2> result = fromSuccessRespV2(resp(new Hash[] {blockHash}));
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0).getBlockAccessList()).isNull();
  }

  @SuppressWarnings("unchecked")
  private List<ExecutionPayloadBodiesV2> fromSuccessRespV2(final JsonRpcResponse resp) {
    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    return (List<ExecutionPayloadBodiesV2>) ((JsonRpcSuccessResponse) resp).getResult();
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
