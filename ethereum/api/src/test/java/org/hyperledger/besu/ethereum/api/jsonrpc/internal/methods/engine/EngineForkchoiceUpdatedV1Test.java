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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EngineForkchoiceUpdatedParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EngineForkchoiceUpdatedV1Test extends AbstractEngineForkchoiceUpdatedTest {

  public EngineForkchoiceUpdatedV1Test() {
    super(EngineForkchoiceUpdatedV1::new);
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_forkchoiceUpdatedV1");
  }

  @Override
  protected String getMethodName() {
    return RpcMethod.ENGINE_FORKCHOICE_UPDATED_V1.getMethodName();
  }

  @Override
  protected RpcErrorType expectedInvalidPayloadError() {
    return RpcErrorType.INVALID_WITHDRAWALS_PARAMS;
  }

  @Test
  public void shouldNotUseV4SpecHelpersForLegacyFlow() {
    final BlockHeader mockParent = blockHeaderBuilder.number(9L).buildHeader();
    final BlockHeader mockHeader =
        blockHeaderBuilder.number(10L).parentHash(mockParent.getHash()).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);
    when(mergeCoordinator.updateForkChoice(any(), any(), any()))
        .thenReturn(ForkchoiceResult.withResult(Optional.of(mockParent), Optional.of(mockHeader)));

    resp(
        new EngineForkchoiceUpdatedParameter(mockHeader.getHash(), Hash.ZERO, mockParent.getHash()),
        Optional.empty());

    verify(mergeCoordinator).updateForkChoice(any(), any(), any());
    verify(mergeCoordinator, never()).updateForkChoiceWithoutLegacySkip(any(), any(), any());
    verify(mergeCoordinator, never()).isAncestorOfFinalized(any());
    verify(mergeCoordinator, never()).computeReorgDepth(any());
  }
}
