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
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetPayloadResultV2;
import org.hyperledger.besu.ethereum.core.Withdrawal;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EngineGetPayloadV2Test extends EngineGetPayloadV1Test {

  @Override
  protected EngineGetPayloadV1 createMethodInstance() {
    return new EngineGetPayloadV2(
        protocolSchedule,
        protocolContext,
        vertx,
        engineCallListener,
        mergeMiningCoordinator,
        factory,
        null,
        CANCUN);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getPayloadV2");
  }

  @Override
  protected void assertPayloadResult(final Object result) {
    super.assertPayloadResult(result);
    assertThat(result).isInstanceOf(EngineGetPayloadResultV2.class);
    final EngineGetPayloadResultV2 res = (EngineGetPayloadResultV2) result;
    assertThat(res.getBlockValue()).isEqualTo(Wei.ZERO);
    assertThat(res.getExecutionPayload()).isInstanceOf(ExecutionPayloadV2.class);
    assertThat(((ExecutionPayloadV2) res.getExecutionPayload()).getWithdrawals()).isNotNull();
  }

  @Test
  public void shouldReturnExecutionPayloadWithoutWithdrawals_PreShanghaiBlock() {
    assumeTrue(supportsPreShanghaiPayloads());

    final PayloadIdentifier preShanghaiPid = setupPayload(shanghaiHardfork.milestone() - 1);

    final var resp = resp(getMethodName(), preShanghaiPid);
    assertThat(resp).isInstanceOf(JsonRpcSuccessResponse.class);
    final EngineGetPayloadResultV2 res =
        (EngineGetPayloadResultV2) ((JsonRpcSuccessResponse) resp).getResult();
    assertThat(res.getExecutionPayload()).isExactlyInstanceOf(ExecutionPayloadV1.class);
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Override
  protected Optional<List<Withdrawal>> defaultWithdrawals() {
    // withdrawals must be present in payloads at or after the Shanghai milestone
    return Optional.of(Collections.emptyList());
  }

  @Override
  protected String getMethodName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_V2.getMethodName();
  }

  @Override
  protected long getValidPayloadTimestamp() {
    return shanghaiHardfork.milestone();
  }

  @Override
  protected OptionalLong getMaxSupportedTimestamp() {
    return OptionalLong.of(cancunHardfork.milestone() - 1);
  }

  protected boolean supportsPreShanghaiPayloads() {
    return true;
  }
}
