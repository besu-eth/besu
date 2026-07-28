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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConstructorArgumentsBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ForkchoiceStateV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.PayloadAttributesV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EngineForkchoiceUpdatedV3Test extends EngineForkchoiceUpdatedV2Test {

  @Override
  protected EngineForkchoiceUpdatedV1<?> createMethodInstance() {
    return new EngineForkchoiceUpdatedV3<>(
        new ConstructorArgumentsBuilder()
            .protocolSchedule(protocolSchedule)
            .protocolContext(protocolContext)
            .vertx(vertx)
            .engineCallListener(engineCallListener)
            .mergeCoordinator(mergeCoordinator)
            .build(),
        CANCUN,
        AMSTERDAM);
  }

  @Override
  @BeforeEach
  public void before() {
    super.before();
    blockHeaderBuilder.timestamp(getMinSupportedTimestamp());
  }

  @Override
  protected long getMinSupportedTimestamp() {
    return cancunHardfork.milestone();
  }

  @Override
  protected OptionalLong getMaxSupportedTimestamp() {
    return OptionalLong.of(amsterdamHardfork.milestone() - 1);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_forkchoiceUpdatedV3");
  }

  @Override
  protected String getMethodName() {
    return RpcMethod.ENGINE_FORKCHOICE_UPDATED_V3.getMethodName();
  }

  @Override
  protected Object validPayloadAttributesForBlock(final BlockHeader head) {
    return new PayloadAttributesV3(
        String.valueOf(head.getTimestamp() + 1),
        Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
        Address.ECREC.toString(),
        Collections.emptyList(),
        Bytes32.ZERO.toHexString());
  }

  @Override
  protected Object invalidTimestampPayloadAttributesForBlock(final BlockHeader head) {
    return new PayloadAttributesV3(
        String.valueOf(head.getTimestamp()),
        Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
        Address.ECREC.toString(),
        Collections.emptyList(),
        Bytes32.ZERO.toHexString());
  }

  @Override
  protected Object payloadAttributesWithNullWithdrawalsForBlock(final BlockHeader head) {
    return new PayloadAttributesV3(
        String.valueOf(head.getTimestamp() + 1),
        Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
        Address.ECREC.toString(),
        null,
        Bytes32.ZERO.toHexString());
  }

  // ---- V3-specific tests ----

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsBeforeSupportedForkWindow() {
    getMaxSupportedTimestamp()
        .ifPresent(
            timestamp -> {
              final BlockHeader mockHeader =
                  setupValidForkchoiceUpdate(bhb -> bhb.timestamp(timestamp + 1));

              final JsonRpcResponse resp =
                  resp(
                      new ForkchoiceStateV1(mockHeader.getBlockHash(), Hash.ZERO, Hash.ZERO),
                      Optional.of(validPayloadAttributesForBlock(mockHeader)));

              final JsonRpcError jsonRpcError =
                  Optional.of(resp)
                      .map(JsonRpcErrorResponse.class::cast)
                      .map(JsonRpcErrorResponse::getError)
                      .get();
              assertThat(jsonRpcError.getCode()).isEqualTo(RpcErrorType.UNSUPPORTED_FORK.getCode());
            });
  }

  @Override
  protected void defaultBlockHeaderCustomization(
      final BlockHeaderTestFixture blockHeaderTestFixture) {
    super.defaultBlockHeaderCustomization(blockHeaderTestFixture);
    blockHeaderTestFixture.timestamp(getMinSupportedTimestamp());
  }

  @Override
  protected long getDefaultTimestamp() {
    return getMinSupportedTimestamp();
  }
}
