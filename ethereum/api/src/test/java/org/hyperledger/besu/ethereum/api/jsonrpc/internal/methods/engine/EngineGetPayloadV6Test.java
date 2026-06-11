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
import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcObjectMapperFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetPayloadResultV6;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotRead;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EngineGetPayloadV6Test extends EngineGetPayloadV5Test {

  private static final ObjectMapper OBJECT_MAPPER =
      JsonRpcObjectMapperFactory.createResponseMapper();

  @Override
  protected EngineGetPayloadV1 createMethodInstance() {
    return new EngineGetPayloadV6(
        protocolSchedule,
        protocolContext,
        vertx,
        engineCallListener,
        mergeMiningCoordinator,
        factory,
        AMSTERDAM,
        null);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getPayloadV6");
  }

  @Override
  protected void assertPayloadResult(final Object result) {
    super.assertPayloadResult(result);
    assertThat(result).isInstanceOf(EngineGetPayloadResultV6.class);
    final EngineGetPayloadResultV6 res = (EngineGetPayloadResultV6) result;
    assertThat(res.getExecutionPayload()).isInstanceOf(ExecutionPayloadV4.class);
    final BlockAccessList blockAccessList = defaultBlockAccessList().orElseThrow();
    assertThat(res.getExecutionPayload().getBlockAccessList()).isEqualTo(blockAccessList);
    final Map<String, Object> wireResult =
        OBJECT_MAPPER.convertValue(res, new TypeReference<>() {});
    assertThat(wireResult.get("executionPayload"))
        .asInstanceOf(map(String.class, Object.class))
        .containsEntry("blockAccessList", encodeBlockAccessList(blockAccessList));
  }

  private static BlockAccessList createSampleBlockAccessList() {
    final Address address = Address.fromHexString("0x0000000000000000000000000000000000000001");
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.ONE);
    final SlotChanges slotChanges =
        new SlotChanges(slotKey, List.of(new StorageChange(0, UInt256.valueOf(2))));
    return new BlockAccessList(
        List.of(
            new AccountChanges(
                address,
                List.of(slotChanges),
                List.of(new SlotRead(slotKey)),
                List.of(new BalanceChange(0, Wei.ONE)),
                List.of(new NonceChange(0, 1L)),
                List.of(new CodeChange(0, Bytes.of(1))))));
  }

  private static String encodeBlockAccessList(final BlockAccessList blockAccessList) {
    final var output = new BytesValueRLPOutput();
    blockAccessList.writeTo(output);
    return output.encoded().toHexString();
  }

  @Override
  protected String getMethodName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_V6.getMethodName();
  }

  @Override
  protected List<Request> getExecutionRequests(final Object result) {
    assertThat(result).isInstanceOf(EngineGetPayloadResultV6.class);
    return ((EngineGetPayloadResultV6) result).getExecutionRequests();
  }

  @Override
  protected long getMinSupportedTimestamp() {
    return amsterdamHardfork.milestone();
  }

  @Override
  protected OptionalLong getFirstUnsupportedTimestamp() {
    return OptionalLong.empty();
  }

  @Override
  protected Optional<BlockAccessList> defaultBlockAccessList() {
    return Optional.of(createSampleBlockAccessList());
  }

  @Override
  protected BlockHeaderTestFixture blockHeaderTestFixture() {
    return super.blockHeaderTestFixture().slotNumber(1L);
  }
}
