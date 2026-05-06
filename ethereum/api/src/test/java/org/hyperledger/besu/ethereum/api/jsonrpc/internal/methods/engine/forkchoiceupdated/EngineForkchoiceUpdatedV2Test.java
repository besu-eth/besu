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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.forkchoiceupdated;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.SHANGHAI;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.engine.ForkchoiceStateV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.engine.PayloadAttributesV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsValidator;

import java.util.Collections;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EngineForkchoiceUpdatedV2Test extends EngineForkchoiceUpdatedV1Test {

  static final long CANCUN_MILESTONE = 1_000_000L;

  @Override
  protected EngineForkchoiceUpdatedV1<?> createMethodInstance() {
    return new EngineForkchoiceUpdatedV2<>(
        vertx,
        protocolSchedule,
        protocolContext,
        mergeCoordinator,
        engineCallListener,
        SHANGHAI,
        CANCUN);
  }

  @Override
  @BeforeEach
  public void before() {
    super.before();
    // CANCUN upper bound; stub after super.before() for LIFO precedence.
    // AllowedWithdrawals needed so inherited "valid with payload" tests pass.
    when(protocolSchedule.milestoneFor(CANCUN)).thenReturn(Optional.of(CANCUN_MILESTONE));
    when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.AllowedWithdrawals());
    createMethod();
  }

  @Override
  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_forkchoiceUpdatedV2");
  }

  @Override
  protected String getMethodName() {
    return RpcMethod.ENGINE_FORKCHOICE_UPDATED_V2.getMethodName();
  }

  @Override
  protected Object validPayloadAttributesForBlock(final BlockHeader head) {
    return new PayloadAttributesV2(
        String.valueOf(head.getTimestamp() + 1),
        Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
        Address.ECREC.toString(),
        Collections.emptyList());
  }

  @Override
  protected Object invalidTimestampPayloadAttributesForBlock(final BlockHeader head) {
    return new PayloadAttributesV2(
        String.valueOf(head.getTimestamp()),
        Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
        Address.ECREC.toString(),
        Collections.emptyList());
  }

  // ---- V2-specific tests ----

  @Test
  public void shouldReturnUnsupportedForkIfBlockTimestampIsAfterCancunMilestone() {
    // head.timestamp = CANCUN_MILESTONE so payload = CANCUN_MILESTONE+1 (> head)
    // and CANCUN_MILESTONE+1 >= CANCUN_MILESTONE triggers UNSUPPORTED_FORK for V2
    final BlockHeader mockHeader = blockHeaderBuilder.timestamp(CANCUN_MILESTONE).buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(mockHeader.getBlockHash(), Hash.ZERO, Hash.ZERO),
            Optional.of(validPayloadAttributesForBlock(mockHeader)));

    assertInvalidForkchoiceState(resp, RpcErrorType.UNSUPPORTED_FORK);
  }

  @Test
  public void shouldReturnInvalidIfWithdrawalsIsNotNull_WhenWithdrawalsProhibited() {
    // Override AllowedWithdrawals (from before()) back to ProhibitedWithdrawals for this test.
    when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.ProhibitedWithdrawals());

    final BlockHeader mockHeader = blockHeaderBuilder.buildHeader();
    setupValidForkchoiceUpdate(mockHeader);

    // validPayloadAttributesForBlock returns non-null withdrawals (emptyList) — prohibited
    final JsonRpcResponse resp =
        resp(
            new ForkchoiceStateV1(mockHeader.getHash(), Hash.ZERO, Hash.ZERO),
            Optional.of(validPayloadAttributesForBlock(mockHeader)));

    assertInvalidForkchoiceState(resp, RpcErrorType.INVALID_PAYLOAD_ATTRIBUTES);
  }

  @Test
  public void shouldReturnValidIfWithdrawalsIsEmpty_WhenWithdrawalsAllowed() {
    // AllowedWithdrawals already set in before(); validPayloadAttributesForBlock returns emptyList
    final BlockHeader mockHeader = blockHeaderBuilder.buildHeader();
    setupValidForkchoiceUpdate(mockHeader);
    when(mergeCoordinator.preparePayload(any())).thenReturn(new PayloadIdentifier(1337L));

    assertSuccessWithPayloadForForkchoiceResult(
        new ForkchoiceStateV1(mockHeader.getHash(), Hash.ZERO, Hash.ZERO),
        Optional.of(validPayloadAttributesForBlock(mockHeader)),
        ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)),
        org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod
            .EngineStatus.VALID);
  }
}
