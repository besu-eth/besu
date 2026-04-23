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
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EnginePayloadStatusResult;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.ScheduledProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotRead;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.mainnet.requests.MainnetRequestsValidator;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EngineNewPayloadV5Test extends EngineNewPayloadV4Test {

  private static final BlockAccessList BLOCK_ACCESS_LIST = createSampleBlockAccessList();
  private static final String ENCODED_BLOCK_ACCESS_LIST = encodeBlockAccessList(BLOCK_ACCESS_LIST);
  private static final String INVALID_BLOCK_ACCESS_LIST_ENCODING = "0xzz";
  private static final String INVALID_BLOCK_ACCESS_LIST_RLP = "0x01";

  @BeforeEach
  @Override
  public void before() {
    super.before();
    lenient().when(protocolSchedule.hardforkFor(any())).thenReturn(Optional.of(amsterdamHardfork));
    lenient()
        .when(protocolSchedule.milestoneFor(AMSTERDAM))
        .thenReturn(Optional.of(amsterdamHardfork.milestone()));
    lenient().when(protocolSpec.getGasCalculator()).thenReturn(new PragueGasCalculator());
    lenient().when(protocolSpec.getRequestsValidator()).thenReturn(new MainnetRequestsValidator());
    final NewPayloadProcessor processor =
        new NewPayloadProcessor(
            protocolSchedule, protocolContext, mergeCoordinator, ethPeers, new NoOpMetricsSystem());
    this.method =
        new EngineNewPayloadV5(
            vertx, protocolSchedule, protocolContext, engineCallListener, processor);
  }

  @Override
  protected Set<ScheduledProtocolSpec.Hardfork> supportedHardforks() {
    return Set.of(amsterdamHardfork);
  }

  @Override
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_newPayloadV5");
  }

  @Override
  @org.junit.jupiter.api.Disabled(
      "V5 does not extend AbstractEngineNewPayload; validateParameters lives on NewPayloadProcessor. Equivalent coverage comes from the end-to-end V5 tests in this class.")
  public void validateVersionedHash_whenListIsPresentAndEmpty() {}

  @Override
  @org.junit.jupiter.api.Disabled(
      "V5 does not extend AbstractEngineNewPayload; validateParameters lives on NewPayloadProcessor. Equivalent coverage comes from the end-to-end V5 tests in this class.")
  public void validateExecutionRequests_whenPresent() {}

  @Override
  public void shouldReturnUnsupportedForkIfBlockTimestampIsAtOrAfterAmsterdamMilestone() {
    final BlockHeader header =
        setupValidPayload(
            new BlockProcessingResult(
                Optional.of(
                    new BlockProcessingOutputs(null, List.of(), Optional.of(VALID_REQUESTS)))),
            Optional.empty());
    when(blockchain.getBlockHeader(header.getParentHash()))
        .thenReturn(Optional.of(mock(BlockHeader.class)));
    when(mergeCoordinator.getLatestValidAncestor(header)).thenReturn(Optional.of(header.getHash()));

    final JsonRpcResponse resp = resp(mockEnginePayload(header, emptyList()));

    assertValidResponse(header, resp);
  }

  @Test
  public void shouldReturnInvalidIfBlockAccessListIsMissing() {
    final BlockHeader header = createValidBlockHeader(Optional.empty());

    final JsonRpcResponse resp = resp(super.mockEnginePayload(header, emptyList(), null, null));

    final EnginePayloadStatusResult result = fromSuccessResp(resp);
    assertThat(result.getStatusAsString()).isEqualTo(INVALID.name());
    assertThat(result.getError()).isEqualTo("Missing block access list field");
    assertThat(result.getLatestValidHash()).isEmpty();
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidIfBlockAccessListHasInvalidHexEncoding() {
    final BlockHeader header = createValidBlockHeader(Optional.empty());

    final JsonRpcResponse resp =
        resp(
            super.mockEnginePayload(header, emptyList(), null, INVALID_BLOCK_ACCESS_LIST_ENCODING));

    final EnginePayloadStatusResult result = fromSuccessResp(resp);
    assertThat(result.getStatusAsString()).isEqualTo(INVALID.name());
    assertThat(result.getError()).isEqualTo("Invalid block access list encoding");
    assertThat(result.getLatestValidHash()).isEmpty();
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnInvalidIfBlockAccessListHasInvalidRlpEncoding() {
    final BlockHeader header = createValidBlockHeader(Optional.empty());

    final JsonRpcResponse resp =
        resp(super.mockEnginePayload(header, emptyList(), null, INVALID_BLOCK_ACCESS_LIST_RLP));

    final EnginePayloadStatusResult result = fromSuccessResp(resp);
    assertThat(result.getStatusAsString()).isEqualTo(INVALID.name());
    assertThat(result.getError()).isEqualTo("Invalid block access list encoding");
    assertThat(result.getLatestValidHash()).isEmpty();
    verify(engineCallListener, times(1)).executionEngineCalled();
  }

  @Test
  public void shouldReturnValidIfBlockAccessListMatchesHeader() {
    assertThat(BLOCK_ACCESS_LIST.accountChanges()).isNotEmpty();

    final BlockHeader header =
        setupValidPayload(
            new BlockProcessingResult(
                Optional.of(
                    new BlockProcessingOutputs(null, List.of(), Optional.of(VALID_REQUESTS)))),
            Optional.empty());

    when(blockchain.getBlockHeader(header.getParentHash()))
        .thenReturn(Optional.of(mock(BlockHeader.class)));
    when(mergeCoordinator.getLatestValidAncestor(header)).thenReturn(Optional.of(header.getHash()));
    when(mergeContext.isSyncing()).thenReturn(false);

    final JsonRpcResponse resp = resp(mockEnginePayload(header, emptyList()));

    assertValidResponse(header, resp);
  }

  @Override
  protected EnginePayloadParameter mockEnginePayload(
      final BlockHeader header, final List<String> txs) {
    return super.mockEnginePayload(header, txs, null, ENCODED_BLOCK_ACCESS_LIST);
  }

  @Override
  protected EnginePayloadParameter mockEnginePayload(
      final BlockHeader header,
      final List<String> txs,
      final List<WithdrawalParameter> withdrawals) {
    return super.mockEnginePayload(header, txs, withdrawals, ENCODED_BLOCK_ACCESS_LIST);
  }

  @Override
  protected BlockHeader createValidBlockHeader(final Optional<List<Withdrawal>> maybeWithdrawals) {
    return createActivationBlockHeaderFixture(maybeWithdrawals)
        .timestamp(amsterdamHardfork.milestone())
        .buildHeader();
  }

  @Override
  protected BlockHeaderTestFixture createActivationBlockHeaderFixture(
      final Optional<List<Withdrawal>> maybeWithdrawals) {
    final BlockHeader parentBlockHeader =
        new BlockHeaderTestFixture()
            .baseFeePerGas(Wei.ONE)
            .timestamp(amsterdamHardfork.milestone() - 2)
            .excessBlobGas(BlobGas.ZERO)
            .blobGasUsed(0L)
            .buildHeader();

    return new BlockHeaderTestFixture()
        .baseFeePerGas(Wei.ONE)
        .parentHash(parentBlockHeader.getParentHash())
        .number(parentBlockHeader.getNumber() + 1)
        .timestamp(parentBlockHeader.getTimestamp() + 1)
        .withdrawalsRoot(maybeWithdrawals.map(BodyValidation::withdrawalsRoot).orElse(null))
        .excessBlobGas(BlobGas.ZERO)
        .blobGasUsed(0L)
        .parentBeaconBlockRoot(
            maybeParentBeaconBlockRoot.isPresent() ? maybeParentBeaconBlockRoot : null)
        .balHash(BodyValidation.balHash(BLOCK_ACCESS_LIST))
        .requestsHash(BodyValidation.requestsHash(VALID_REQUESTS))
        .slotNumber(1L);
  }

  @Override
  protected BlockHeader createPreActivationBlockHeader() {
    return createActivationBlockHeaderFixture(Optional.empty())
        .timestamp(amsterdamHardfork.milestone() - 1)
        .buildHeader();
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
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    blockAccessList.writeTo(output);
    return output.encoded().toHexString();
  }
}
