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
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType.UNSUPPORTED_FORK;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EngineForkchoiceUpdatedParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadAttributesParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EngineForkchoiceUpdatedV2Test extends AbstractEngineForkchoiceUpdatedTest {

  public EngineForkchoiceUpdatedV2Test() {
    super(EngineForkchoiceUpdatedV2::new);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_forkchoiceUpdatedV2");
  }

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsAfterCancunMilestone() {
    when(mergeCoordinator.updateForkChoice(
            any(BlockHeader.class), any(Hash.class), any(Hash.class)))
        .thenReturn(mock(MergeMiningCoordinator.ForkchoiceResult.class));

    BlockHeader mockHeader = blockHeaderBuilder.timestamp(CANCUN_MILESTONE + 1).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    final EngineForkchoiceUpdatedParameter param =
        new EngineForkchoiceUpdatedParameter(mockHeader.getBlockHash(), Hash.ZERO, Hash.ZERO);

    final EnginePayloadAttributesParameter payloadParams =
        new EnginePayloadAttributesParameter(
            String.valueOf(mockHeader.getTimestamp()),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            null,
            null,
            null);

    final JsonRpcResponse resp = resp(param, Optional.of(payloadParams));

    final JsonRpcError jsonRpcError =
        Optional.of(resp)
            .map(JsonRpcErrorResponse.class::cast)
            .map(JsonRpcErrorResponse::getError)
            .get();
    assertThat(jsonRpcError.getCode()).isEqualTo(UNSUPPORTED_FORK.getCode());
  }

  @Override
  protected String getMethodName() {
    return RpcMethod.ENGINE_FORKCHOICE_UPDATED_V2.getMethodName();
  }

  @Override
  protected RpcErrorType expectedInvalidPayloadError() {
    return RpcErrorType.INVALID_PAYLOAD_ATTRIBUTES;
  }
}
