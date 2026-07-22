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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PRAGUE;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConstructorArgumentsBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetPayloadResultV3;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.OptionalLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class})
public class EngineGetPayloadV3Test extends EngineGetPayloadV2Test {

  @Override
  protected EngineGetPayloadV1 createMethodInstance() {
    return new EngineGetPayloadV3(
        new ConstructorArgumentsBuilder()
            .protocolSchedule(protocolSchedule)
            .protocolContext(protocolContext)
            .vertx(vertx)
            .engineCallListener(engineCallListener)
            .mergeCoordinator(mergeMiningCoordinator)
            .blockResultFactory(factory)
            .maxRequestBlocks(0)
            .build(),
        CANCUN,
        PRAGUE);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getPayloadV3");
  }

  @Override
  protected void assertPayloadResult(final Object result) {
    super.assertPayloadResult(result);
    assertThat(result).isInstanceOf(EngineGetPayloadResultV3.class);
    final EngineGetPayloadResultV3 res = (EngineGetPayloadResultV3) result;
    assertThat(res.getExecutionPayload()).isInstanceOf(ExecutionPayloadV3.class);
    assertThat(res.getExecutionPayload().getExcessBlobGas())
        .isEqualTo(mockHeader.getExcessBlobGas().orElseThrow());
    assertThat(res.getBlobsBundle()).isNotNull();
  }

  @Override
  protected BlockHeaderTestFixture blockHeaderTestFixture() {
    return super.blockHeaderTestFixture().excessBlobGas(BlobGas.of(10L)).blobGasUsed(0L);
  }

  @Override
  protected String getMethodName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_V3.getMethodName();
  }

  @Override
  protected long getValidPayloadTimestamp() {
    return getMinSupportedTimestamp();
  }

  @Override
  protected long getMinSupportedTimestamp() {
    return cancunHardfork.milestone();
  }

  @Override
  protected OptionalLong getMaxSupportedTimestamp() {
    return OptionalLong.of(pragueHardfork.milestone() - 1);
  }

  @Override
  protected boolean validatesMinSupportedFork() {
    return true;
  }

  @Override
  protected boolean supportsPreShanghaiPayloads() {
    return false;
  }
}
