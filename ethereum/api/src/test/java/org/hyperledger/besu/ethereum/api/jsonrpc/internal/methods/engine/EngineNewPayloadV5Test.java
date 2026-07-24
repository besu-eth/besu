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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BOGOTA;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineTestSupport.fromErrorResp;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.INVALID_BLOCK_ACCESS_LIST_PARAMS;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConstructorArgumentsBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.UnaryOperator;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class EngineNewPayloadV5Test extends EngineNewPayloadV4Test {

  private static final BlockAccessList BLOCK_ACCESS_LIST = createSampleBlockAccessList();
  private static final String INVALID_BLOCK_ACCESS_LIST_ENCODING = "0xzz";
  private static final String INVALID_BLOCK_ACCESS_LIST_RLP = "0x01";

  @BeforeEach
  @Override
  public void before() {
    super.before();
  }

  @Override
  protected EngineNewPayloadV1<?, ?> createMethodInstance() {
    return new EngineNewPayloadV5<>(
        new ConstructorArgumentsBuilder()
            .protocolSchedule(protocolSchedule)
            .protocolContext(protocolContext)
            .vertx(vertx)
            .engineCallListener(engineCallListener)
            .mergeCoordinator(mergeCoordinator)
            .ethPeers(ethPeers)
            .metricsSystem(new NoOpMetricsSystem())
            .build(),
        AMSTERDAM,
        BOGOTA);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_newPayloadV5");
  }

  @Override
  @Test
  @Disabled("blockAccessList becomes a required field from V5 (engine_newPayloadV5) onward")
  public void shouldReturnInvalidParamsIfBlockAccessListPresent() {}

  @Override
  protected long getMinSupportedTimestamp() {
    return amsterdamHardfork.milestone();
  }

  @Override
  protected OptionalLong getMaxSupportedTimestamp() {
    return OptionalLong.empty();
  }

  @Test
  public void shouldReturnInvalidIfBlockAccessListIsMissing() {
    final BlockHeader header =
        setupPayloadV5(
            getMinSupportedTimestamp(), new BlockProcessingResult(Optional.empty()), null, 0L);

    final JsonRpcResponse resp =
        respV5(mockEnginePayloadParam(header, emptyList(), (BlockAccessList) null, 0L));

    final var errorResp = fromErrorResp(resp);
    assertThat(errorResp.getCode()).isEqualTo(INVALID_BLOCK_ACCESS_LIST_PARAMS.getCode());
    assertThat(errorResp.getMessage())
        .isEqualTo("Invalid block access list params (missing or invalid)");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidIfBlockAccessListHasInvalidHexEncoding() {
    final BlockHeader header =
        setupPayloadV5(
            getMinSupportedTimestamp(), new BlockProcessingResult(Optional.empty()), null, 0L);

    final Map<String, Object> payloadParam =
        mockEnginePayloadParam(header, emptyList(), (BlockAccessList) null, 0L);

    payloadParam.put("blockAccessList", INVALID_BLOCK_ACCESS_LIST_ENCODING);

    final JsonRpcResponse resp = respV5(payloadParam);

    final var errorResp = fromErrorResp(resp);
    assertThat(errorResp.getCode()).isEqualTo(INVALID_BLOCK_ACCESS_LIST_PARAMS.getCode());
    assertThat(errorResp.getMessage())
        .isEqualTo("Invalid block access list params (missing or invalid)");
    assertThat(errorResp.getData())
        .isEqualTo(
            "Failed to decode block access list payload parameter (Illegal character 'z' found at index 0 in hex binary representation)");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidIfBlockAccessListHasInvalidRlpEncoding() {
    final BlockHeader header =
        setupPayloadV5(
            getMinSupportedTimestamp(), new BlockProcessingResult(Optional.empty()), null, 0L);

    final Map<String, Object> payloadParam =
        mockEnginePayloadParam(header, emptyList(), (BlockAccessList) null, 0L);

    payloadParam.put("blockAccessList", INVALID_BLOCK_ACCESS_LIST_RLP);

    final JsonRpcResponse resp = respV5(payloadParam);

    final var errorResp = fromErrorResp(resp);
    assertThat(errorResp.getCode()).isEqualTo(INVALID_BLOCK_ACCESS_LIST_PARAMS.getCode());
    assertThat(errorResp.getMessage())
        .isEqualTo("Invalid block access list params (missing or invalid)");
    assertThat(errorResp.getData())
        .isEqualTo(
            "Failed to decode block access list payload parameter (Expected current item to be a list, but it is: BYTE_ELEMENT (at bytes 0-1: [01]))");
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnValidIfBlockAccessListMatchesHeader() {
    assertThat(BLOCK_ACCESS_LIST.accountChanges()).isNotEmpty();

    final BlockHeader header =
        setupPayloadV5(
            getMinSupportedTimestamp(),
            new BlockProcessingResult(Optional.empty()),
            BLOCK_ACCESS_LIST,
            0L);

    final JsonRpcResponse resp =
        respV5(mockEnginePayloadParam(header, emptyList(), BLOCK_ACCESS_LIST, 0L));

    assertValidResponse(header, resp);
  }

  protected BlockHeader setupPayloadV5(
      final long timestamp,
      final BlockProcessingResult value,
      final BlockAccessList blockAccessList,
      final Long slotNumber) {
    return setupPayloadV5(timestamp, value, blockAccessList, slotNumber, UnaryOperator.identity());
  }

  protected BlockHeader setupPayloadV5(
      final long timestamp,
      final BlockProcessingResult value,
      final BlockAccessList blockAccessList,
      final Long slotNumber,
      final UnaryOperator<BlockHeaderTestFixture> nextVersionSpecificModifier) {
    return setupPayloadV4(
        timestamp,
        value,
        emptyList(),
        fixture ->
            nextVersionSpecificModifier.apply(
                setBlockAccessListAndSlotNumberField(fixture, blockAccessList, slotNumber)));
  }

  protected Map<String, Object> mockEnginePayloadParam(
      final BlockHeader header,
      final List<String> txs,
      final BlockAccessList blockAccessList,
      final Long slotNumber) {
    var param = super.mockEnginePayloadParam(header, txs, BlobGas.ZERO, 0L);
    if (blockAccessList != null) {
      param.put("blockAccessList", blockAccessListAsParam(blockAccessList));
    } else {
      param.remove("blockAccessList");
    }
    if (slotNumber != null) {
      param.put("slotNumber", Bytes.ofUnsignedLong(slotNumber).toHexString());
    } else {
      param.remove("slotNumber");
    }
    return param;
  }

  @Override
  protected void setDefaultExecutionPayloadFields(
      final Map<String, Object> payload, final BlockHeader header, final List<String> txs) {
    super.setDefaultExecutionPayloadFields(payload, header, txs);
    payload.put("blockAccessList", blockAccessListAsParam(new BlockAccessList(emptyList())));
    payload.put("slotNumber", Bytes.ofUnsignedLong(header.getSlotNumber()).toHexString());
  }

  @Override
  protected BlockHeaderTestFixture versionSpecificBlockHeaderFixture(final long timestamp) {
    BlockHeaderTestFixture baseFixture = super.versionSpecificBlockHeaderFixture(timestamp);

    return setBlockAccessListAndSlotNumberField(baseFixture, new BlockAccessList(emptyList()), 0L);
  }

  private BlockHeaderTestFixture setBlockAccessListAndSlotNumberField(
      final BlockHeaderTestFixture fixture,
      final BlockAccessList blockAccessList,
      final Long slotNumber) {
    if (blockAccessList != null) {
      fixture.balHash(BodyValidation.balHash(blockAccessList));
    }
    if (slotNumber != null) {
      fixture.slotNumber(slotNumber);
    }
    return fixture;
  }

  protected JsonRpcResponse respV5(final Map<String, Object> payloadParam) {
    return resp(
        payloadParam,
        emptyVersionedHashesParam(),
        zeroParentBeaconBlockRootParam(),
        emptyRequestsParam());
  }

  protected String blockAccessListAsParam(final BlockAccessList blockAccessList) {
    var rlpOutput = new BytesValueRLPOutput();
    blockAccessList.writeTo(rlpOutput);
    return rlpOutput.encoded().toHexString();
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
                List.of(new BlockAccessList.NonceChange(0, 1L)),
                List.of(new BlockAccessList.CodeChange(0, Bytes.of(1))))));
  }
}
