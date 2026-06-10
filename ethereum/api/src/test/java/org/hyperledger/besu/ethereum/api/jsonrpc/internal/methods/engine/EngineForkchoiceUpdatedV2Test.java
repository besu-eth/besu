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

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.PARIS;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ForkchoiceStateV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.PayloadAttributesV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ForkchoiceUpdatedResultV1;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsValidator;

import java.util.Optional;
import java.util.OptionalLong;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EngineForkchoiceUpdatedV2Test extends EngineForkchoiceUpdatedV1Test {

  private final long withdrawalsEnabledTimestamp = shanghaiHardfork.milestone();

  @Override
  protected EngineForkchoiceUpdatedV1<?> createMethodInstance() {
    return new EngineForkchoiceUpdatedV2<>(
        protocolSchedule,
        protocolContext,
        vertx,
        engineCallListener,
        mergeCoordinator,
        PARIS,
        CANCUN);
  }

  @Override
  @BeforeEach
  public void before() {
    super.before();
    when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.AllowedWithdrawals());
    blockHeaderBuilder.timestamp(withdrawalsEnabledTimestamp);
  }

  @Override
  protected long getMinSupportedTimestamp() {
    return parisHardfork.milestone();
  }

  @Override
  protected OptionalLong getMaxSupportedTimestamp() {
    return OptionalLong.of(cancunHardfork.milestone() - 1);
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
    // if called with a timestamp before, Shanghai should behave like V1
    if (head.getTimestamp() < withdrawalsEnabledTimestamp) {
      when(protocolSpec.getWithdrawalsValidator())
          .thenReturn(new WithdrawalsValidator.ProhibitedWithdrawals());
      return super.validPayloadAttributesForBlock(head);
    }
    return validPayloadAttributesForBlockV2(head);
  }

  private PayloadAttributesV2 validPayloadAttributesForBlockV2(final BlockHeader head) {
    return new PayloadAttributesV2(
        String.valueOf(head.getTimestamp() + 1),
        Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
        Address.ECREC.toString(),
        emptyList());
  }

  @Override
  protected Object invalidTimestampPayloadAttributesForBlock(final BlockHeader head) {
    return new PayloadAttributesV2(
        String.valueOf(head.getTimestamp()),
        Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
        Address.ECREC.toString(),
        emptyList());
  }

  // ---- V2-specific tests ----

  @Test
  public void shouldReturnInvalidIfWithdrawalsIsNotNull_WhenWithdrawalsProhibited() {
    if (getMinSupportedTimestamp() < withdrawalsEnabledTimestamp) {
      // Override AllowedWithdrawals (from before()) back to ProhibitedWithdrawals for this test.
      when(protocolSpec.getWithdrawalsValidator())
          .thenReturn(new WithdrawalsValidator.ProhibitedWithdrawals());

      final BlockHeader mockHeader =
          setupValidForkchoiceUpdate(bhb -> bhb.timestamp(withdrawalsEnabledTimestamp / 2));

      final JsonRpcResponse resp =
          resp(
              new ForkchoiceStateV1(mockHeader.getHash(), Hash.ZERO, Hash.ZERO),
              // validPayloadAttributesForBlockV2 returns non-null withdrawals (emptyList) —
              // prohibited
              Optional.of(validPayloadAttributesForBlockV2(mockHeader)));

      assertInvalidForkchoiceState(resp, RpcErrorType.INVALID_PAYLOAD_ATTRIBUTES);
    }
  }

  @Test
  public void shouldReturnValidIfWithdrawalsIsEmpty_WhenWithdrawalsAllowed() {
    // AllowedWithdrawals already set in before(); validPayloadAttributesForBlock returns emptyList
    final BlockHeader mockHeader = setupValidForkchoiceUpdate(_ -> {});
    when(mergeCoordinator.preparePayload(any())).thenReturn(new PayloadIdentifier(1337L));

    assertSuccessWithPayloadForForkchoiceResult(
        new ForkchoiceStateV1(mockHeader.getHash(), Hash.ZERO, Hash.ZERO),
        Optional.of(validPayloadAttributesForBlock(mockHeader)),
        ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)),
        VALID);
  }

  @Test
  public void shouldReturnValidIfWithdrawalsIsNotNull_WhenWithdrawalsAllowed() {
    final BlockHeader mockHeader = setupValidForkchoiceUpdate();

    final ForkchoiceUpdatedResultV1 resp =
        assertSuccessWithPayloadForForkchoiceResult(
            new ForkchoiceStateV1(mockHeader.getHash(), Hash.ZERO, Hash.ZERO),
            Optional.of(validPayloadAttributesForBlock(mockHeader)),
            ForkchoiceResult.withResult(Optional.empty(), Optional.of(mockHeader)),
            VALID);

    assertThat(resp.getPayloadId()).isNotNull();
  }

  @Override
  protected void defaultBlockHeaderCustomization(
      final BlockHeaderTestFixture blockHeaderTestFixture) {
    super.defaultBlockHeaderCustomization(blockHeaderTestFixture);
    blockHeaderTestFixture.timestamp(withdrawalsEnabledTimestamp);
  }

  @Override
  protected long getDefaultTimestamp() {
    return withdrawalsEnabledTimestamp;
  }
}
