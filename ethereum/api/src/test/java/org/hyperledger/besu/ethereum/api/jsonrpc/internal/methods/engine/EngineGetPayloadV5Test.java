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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.OSAKA;

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConstructorArgumentsBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlobsBundleV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetPayloadResultV5;
import org.hyperledger.besu.ethereum.core.Request;

import java.util.List;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class})
public class EngineGetPayloadV5Test extends EngineGetPayloadV4Test {

  @Override
  protected EngineGetPayloadV1 createMethodInstance() {
    return new EngineGetPayloadV5(
        new ConstructorArgumentsBuilder()
            .protocolSchedule(protocolSchedule)
            .protocolContext(protocolContext)
            .vertx(vertx)
            .engineCallListener(engineCallListener)
            .mergeCoordinator(mergeMiningCoordinator)
            .blockResultFactory(factory)
            .maxRequestBlocks(0)
            .build(),
        OSAKA,
        AMSTERDAM);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getPayloadV5");
  }

  @Override
  protected void assertPayloadResult(final Object result) {
    super.assertPayloadResult(result);
    assertThat(result).isInstanceOf(EngineGetPayloadResultV5.class);
    assertThat(((EngineGetPayloadResultV5) result).getBlobsBundle())
        .isInstanceOf(BlobsBundleV2.class);
  }

  @Override
  protected List<Request> getExecutionRequests(final Object result) {
    assertThat(result).isInstanceOf(EngineGetPayloadResultV5.class);
    return ((EngineGetPayloadResultV5) result).getExecutionRequests();
  }

  @Override
  protected String getMethodName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_V5.getMethodName();
  }

  @Override
  protected long getMinSupportedTimestamp() {
    return osakaHardfork.milestone();
  }

  @Override
  protected OptionalLong getMaxSupportedTimestamp() {
    return OptionalLong.of(amsterdamHardfork.milestone() - 1);
  }
}
