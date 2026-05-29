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

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.PayloadWrapper;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.consensus.merge.blockcreation.PreparePayloadArgsBuilder;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.RequestType;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcObjectMapperFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetPayloadResultV6;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreationTiming;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockWithReceipts;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotRead;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EngineGetPayloadV6Test extends EngineGetPayloadV5Test {

  private static final long AMSTERDAM_TIMESTAMP = 100L;
  private static final ObjectMapper OBJECT_MAPPER =
      JsonRpcObjectMapperFactory.createResponseMapper();

  @Override
  protected void setupVersionSpecificMocks() {
    super.setupVersionSpecificMocks();
    when(protocolSchedule.milestoneFor(AMSTERDAM)).thenReturn(Optional.of(AMSTERDAM_TIMESTAMP));
  }

  @Override
  protected EngineGetPayloadV1 createMethodInstance() {
    return new EngineGetPayloadV6(
        vertx,
        protocolSchedule,
        protocolContext,
        mergeMiningCoordinator,
        factory,
        engineCallListener,
        AMSTERDAM,
        null);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getPayloadV6");
  }

  @Override
  @Test
  public void shouldReturnBlockForKnownPayloadId() {
    final BlockAccessList blockAccessList = createSampleBlockAccessList();
    final String encodedBlockAccessList = encodeBlockAccessList(blockAccessList);
    final BlockHeader header =
        blockHeaderTestFixture()
            .timestamp(AMSTERDAM_TIMESTAMP + 1)
            .excessBlobGas(BlobGas.ZERO)
            .blobGasUsed(0L)
            .balHash(BodyValidation.balHash(blockAccessList))
            .buildHeader();

    final BlockWithReceipts blockWithReceipts =
        new BlockWithReceipts(
            new Block(
                header,
                new BlockBody(
                    List.of(new TransactionTestFixture().createTransaction(senderKeys)),
                    emptyList(),
                    Optional.of(emptyList()))),
            List.of(mock(TransactionReceipt.class)));

    final PayloadIdentifier payloadIdentifier =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(new BlockHeaderTestFixture().buildHeader())
                .timestamp(header.getTimestamp())
                .prevRandao(Bytes32.random())
                .feeRecipient(Address.fromHexString("0x42"))
                .build());

    final List<Request> requests =
        List.of(
            new Request(RequestType.DEPOSIT, Bytes.of(1)),
            new Request(RequestType.WITHDRAWAL, Bytes.of(1)),
            new Request(RequestType.CONSOLIDATION, Bytes.of(1)));

    final PayloadWrapper payload =
        new PayloadWrapper(
            payloadIdentifier,
            blockWithReceipts,
            Optional.of(blockAccessList),
            Optional.of(requests),
            BlockCreationTiming.EMPTY);

    when(mergeContext.retrievePayloadById(payloadIdentifier)).thenReturn(Optional.of(payload));

    final var resp = resp(RpcMethod.ENGINE_GET_PAYLOAD_V6.getMethodName(), payloadIdentifier);

    assertThat(resp).isInstanceOf(JsonRpcSuccessResponse.class);
    final EngineGetPayloadResultV6 result =
        (EngineGetPayloadResultV6) ((JsonRpcSuccessResponse) resp).getResult();

    assertThat(result.getExecutionPayload()).isInstanceOf(ExecutionPayloadV4.class);
    assertThat(result.getExecutionPayload().getBlockAccessList()).isEqualTo(blockAccessList);
    final Map<String, Object> wireResult =
        OBJECT_MAPPER.convertValue(result, new TypeReference<>() {});
    assertThat(wireResult.get("executionPayload"))
        .asInstanceOf(map(String.class, Object.class))
        .containsEntry("blockAccessList", encodedBlockAccessList);
    assertThat(result.getBlockValue()).isEqualTo(Quantity.create(payload.blockValue()));
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsBeforeEip7928Milestone() {
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .timestamp(AMSTERDAM_TIMESTAMP - 1)
            .excessBlobGas(BlobGas.ZERO)
            .blobGasUsed(0L)
            .buildHeader();

    final PayloadIdentifier payloadIdentifier =
        PayloadIdentifier.forPayloadParams(
            new PreparePayloadArgsBuilder()
                .parentHeader(new BlockHeaderTestFixture().buildHeader())
                .timestamp(header.getTimestamp())
                .prevRandao(Bytes32.random())
                .feeRecipient(Address.fromHexString("0x42"))
                .build());

    final BlockWithReceipts blockWithReceipts =
        new BlockWithReceipts(
            new Block(header, new BlockBody(emptyList(), emptyList())), emptyList());
    final PayloadWrapper payload =
        new PayloadWrapper(
            payloadIdentifier,
            blockWithReceipts,
            Optional.empty(),
            Optional.empty(),
            BlockCreationTiming.EMPTY);

    when(mergeContext.retrievePayloadById(payloadIdentifier)).thenReturn(Optional.of(payload));

    final var resp = resp(RpcMethod.ENGINE_GET_PAYLOAD_V6.getMethodName(), payloadIdentifier);

    assertThat(resp).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) resp).getErrorType())
        .isEqualTo(RpcErrorType.UNSUPPORTED_FORK);
    verify(engineCallListener, times(1)).executionEngineCalled();
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
    final var output = new org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput();
    blockAccessList.writeTo(output);
    return output.encoded().toHexString();
  }

  @Override
  protected String getMethodName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_V6.getMethodName();
  }

  @Override
  protected List<String> getExecutionRequests(final Object result) {
    assertThat(result).isInstanceOf(EngineGetPayloadResultV6.class);
    return ((EngineGetPayloadResultV6) result).getExecutionRequests();
  }

  @Override
  protected long getValidPayloadTimestamp() {
    return AMSTERDAM_TIMESTAMP + 1;
  }

  @Override
  protected OptionalLong getMinSupportedTimestamp() {
    return OptionalLong.of(AMSTERDAM_TIMESTAMP);
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
  protected Optional<List<Request>> defaultRequests() {
    return Optional.of(
        List.of(
            new Request(RequestType.DEPOSIT, Bytes.of(1)),
            new Request(RequestType.WITHDRAWAL, Bytes.of(1)),
            new Request(RequestType.CONSOLIDATION, Bytes.of(1))));
  }

  @Override
  protected BlockHeaderTestFixture blockHeaderTestFixture() {
    return super.blockHeaderTestFixture().slotNumber(1L);
  }
}
