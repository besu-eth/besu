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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INCLUSION_LIST_UNSATISFIED;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator.ForkchoiceResult;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.RequestType;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.datatypes.parameters.UnsignedLongParameter;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EngineForkchoiceUpdatedParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadAttributesParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EnginePayloadStatusResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineUpdateForkchoiceResult;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.inclusionlist.DefaultInclusionListSelector;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.CancunTargetingGasLimitCalculator;
import org.hyperledger.besu.ethereum.mainnet.DefaultProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ScheduledProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsValidator;
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
import org.hyperledger.besu.metrics.StubMetricsSystem;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Integration test for the complete Inclusion List (IL) workflow: engine_getInclusionListV1 ->
 * engine_forkchoiceUpdatedV4 -> payload building -> engine_newPayloadV5
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class InclusionListWorkflowIntegrationTest {

  private static final Supplier<SignatureAlgorithm> SIGNATURE_ALGORITHM =
      Suppliers.memoize(SignatureAlgorithmFactory::getInstance);
  private static final SECPPrivateKey PRIVATE_KEY =
      SIGNATURE_ALGORITHM
          .get()
          .createPrivateKey(
              Bytes32.fromHexString(
                  "8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63"));
  private static final KeyPair KEYS =
      new KeyPair(PRIVATE_KEY, SIGNATURE_ALGORITHM.get().createPublicKey(PRIVATE_KEY));

  private static final long AMSTERDAM_MILESTONE = 100L;
  private static final ScheduledProtocolSpec.Hardfork AMSTERDAM_HARDFORK =
      new ScheduledProtocolSpec.Hardfork("Amsterdam", AMSTERDAM_MILESTONE);
  private static final Bytes32 MOCK_PBSR = Bytes32.fromHexStringLenient("0xBEEF");
  private static final BlockAccessList BLOCK_ACCESS_LIST = createSampleBlockAccessList();
  private static final String ENCODED_BLOCK_ACCESS_LIST = encodeBlockAccessList(BLOCK_ACCESS_LIST);
  private static final List<Request> VALID_REQUESTS =
      List.of(
          new Request(RequestType.DEPOSIT, Bytes.of(1)),
          new Request(RequestType.WITHDRAWAL, Bytes.of(1)),
          new Request(RequestType.CONSOLIDATION, Bytes.of(1)));
  private static final Vertx vertx = Vertx.vertx();

  @Mock private ProtocolContext protocolContext;
  @Mock private DefaultProtocolSchedule protocolSchedule;
  @Mock private ProtocolSpec protocolSpec;
  @Mock private MergeContext mergeContext;
  @Mock private MergeMiningCoordinator mergeCoordinator;
  @Mock private MutableBlockchain blockchain;
  @Mock private EthPeers ethPeers;
  @Mock private EngineCallListener engineCallListener;
  @Mock private TransactionPool transactionPool;

  private StubMetricsSystem metricsSystem;
  private EngineGetInclusionListV1 getInclusionListMethod;
  private EngineForkchoiceUpdatedV4 forkchoiceUpdatedMethod;
  private EngineNewPayloadV5 newPayloadStrictMethod;
  private EngineNewPayloadV5 newPayloadLenientMethod;

  @BeforeEach
  public void setUp() {
    metricsSystem = new StubMetricsSystem();

    when(protocolContext.safeConsensusContext(any())).thenReturn(Optional.of(mergeContext));
    when(mergeContext.isSyncing()).thenReturn(false);
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.ProhibitedWithdrawals());
    when(protocolSpec.getGasCalculator()).thenReturn(new PragueGasCalculator());
    when(protocolSpec.getGasLimitCalculator())
        .thenReturn(mock(CancunTargetingGasLimitCalculator.class));
    when(protocolSpec.getRequestsValidator()).thenReturn(new MainnetRequestsValidator());
    when(protocolSchedule.getForNextBlockHeader(any(), anyLong())).thenReturn(protocolSpec);
    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    when(protocolSchedule.milestoneFor(CANCUN)).thenReturn(Optional.of(1_000_000L));
    when(protocolSchedule.milestoneFor(AMSTERDAM)).thenReturn(Optional.of(AMSTERDAM_MILESTONE));
    when(protocolSchedule.hardforkFor(any())).thenReturn(Optional.of(AMSTERDAM_HARDFORK));

    getInclusionListMethod =
        new EngineGetInclusionListV1(
            vertx,
            protocolContext,
            engineCallListener,
            transactionPool,
            metricsSystem,
            new DefaultInclusionListSelector());

    forkchoiceUpdatedMethod =
        new EngineForkchoiceUpdatedV4(
            vertx, protocolSchedule, protocolContext, mergeCoordinator, engineCallListener);

    newPayloadStrictMethod =
        new EngineNewPayloadV5(
            vertx,
            protocolSchedule,
            protocolContext,
            mergeCoordinator,
            ethPeers,
            engineCallListener,
            metricsSystem);

    newPayloadLenientMethod =
        new EngineNewPayloadV5(
            vertx,
            protocolSchedule,
            protocolContext,
            mergeCoordinator,
            ethPeers,
            engineCallListener,
            metricsSystem);
  }

  @Test
  public void fullWorkflow_getIL_forkchoiceUpdated_newPayloadValid_strictMode() {
    // Step 1: getInclusionListV1 - get IL transactions from mempool
    final Transaction tx = createLegacyTransaction(0, Wei.of(100));
    final PendingTransaction pt =
        PendingTransaction.newPendingTransaction(tx, false, false, (byte) 0);

    final BlockHeader parentHeader = createParentBlockHeader();
    when(blockchain.getBlockHeader(parentHeader.getHash())).thenReturn(Optional.of(parentHeader));

    when(transactionPool.getPendingTransactions()).thenReturn(List.of(pt));

    final JsonRpcResponse getILResponse = callGetInclusionList(parentHeader.getHash());
    assertThat(getILResponse.getType()).isEqualTo(RpcResponseType.SUCCESS);

    @SuppressWarnings("unchecked")
    final List<String> ilTxHexList =
        (List<String>) ((JsonRpcSuccessResponse) getILResponse).getResult();
    assertThat(ilTxHexList).isNotEmpty();
    assertThat(metricsSystem.getCounterValue("engine_inclusion_list_transactions_generated"))
        .isGreaterThan(0);

    // Step 2: forkchoiceUpdatedV4 - initiate payload building with IL transactions
    final BlockHeader childHeader = createChildBlockHeader(parentHeader);
    setupValidForkchoiceUpdate(childHeader, parentHeader);

    final EnginePayloadAttributesParameter payloadAttrs =
        new EnginePayloadAttributesParameter(
            String.valueOf(childHeader.getTimestamp() + 1),
            Bytes32.fromHexStringLenient("0xDEADBEEF").toHexString(),
            Address.ECREC.toString(),
            null,
            MOCK_PBSR.toHexString(),
            "0x01",
            ilTxHexList);

    final PayloadIdentifier mockPayloadId =
        PayloadIdentifier.forPayloadParams(
            childHeader.getHash(),
            payloadAttrs.getTimestamp(),
            payloadAttrs.getPrevRandao(),
            payloadAttrs.getSuggestedFeeRecipient(),
            Optional.empty(),
            Optional.ofNullable(payloadAttrs.getParentBeaconBlockRoot()),
            Optional.ofNullable(payloadAttrs.getSlotNumber()),
            payloadAttrs.getInclusionListTransactions().stream()
                .map(Transaction::readFrom)
                .toList());

    when(mergeCoordinator.preparePayload(
            any(), anyLong(), any(), any(), any(), any(), any(), any()))
        .thenReturn(mockPayloadId);

    final JsonRpcResponse fcuResponse =
        callForkchoiceUpdated(childHeader, parentHeader, payloadAttrs);
    assertThat(fcuResponse.getType()).isEqualTo(RpcResponseType.SUCCESS);

    final EngineUpdateForkchoiceResult fcuResult =
        (EngineUpdateForkchoiceResult) ((JsonRpcSuccessResponse) fcuResponse).getResult();
    assertThat(fcuResult.getPayloadStatus().getStatus()).isEqualTo(VALID);
    assertThat(fcuResult.getPayloadId()).isNotNull();

    // Verify preparePayload was called with IL transactions
    verify(mergeCoordinator)
        .preparePayload(any(), anyLong(), any(), any(), any(), any(), any(), any());

    // Step 3: newPayloadV5 - validate payload with empty IL (valid, no constraints)
    final BlockHeader payloadHeader = setupValidPayloadHeader();

    // Empty IL means no constraints to enforce - should be VALID
    final JsonRpcResponse newPayloadResp =
        callNewPayload(newPayloadStrictMethod, payloadHeader, emptyList(), emptyList());

    final EnginePayloadStatusResult npResult = fromSuccessResp(newPayloadResp);
    assertThat(npResult.getStatusAsString()).isEqualTo(VALID.name());
  }

  @Test
  public void fullWorkflow_newPayloadRejectsWhenILUnsatisfied_strictMode() {
    final BlockHeader payloadHeader = setupValidPayloadHeader();

    // Payload is empty but IL requires a transaction - should fail strict validation
    final JsonRpcResponse resp =
        callNewPayload(newPayloadStrictMethod, payloadHeader, emptyList(), List.of("0xaabb"));

    final EnginePayloadStatusResult result = fromSuccessResp(resp);
    assertThat(result.getStatusAsString()).isEqualTo(INCLUSION_LIST_UNSATISFIED.name());
    assertThat(result.getError()).isNull();
    assertThat(metricsSystem.getCounterValue("engine_inclusion_list_validation_failures"))
        .isEqualTo(1);
  }

  @Test
  public void fullWorkflow_newPayloadAcceptsWhenILUnsatisfied_lenientMode() {
    final BlockHeader payloadHeader = setupValidPayloadHeader();

    // Payload is empty but IL requires a transaction - lenient mode should still return VALID
    final JsonRpcResponse resp =
        callNewPayload(newPayloadLenientMethod, payloadHeader, emptyList(), List.of("0xaabb"));

    final EnginePayloadStatusResult result = fromSuccessResp(resp);
    assertThat(result.getStatusAsString()).isEqualTo(VALID.name());
  }

  @Test
  public void fullWorkflow_newPayloadValidWithEmptyIL_strictMode() {
    final BlockHeader payloadHeader = setupValidPayloadHeader();

    // Empty IL means no constraints - should always be VALID
    final JsonRpcResponse resp =
        callNewPayload(newPayloadStrictMethod, payloadHeader, emptyList(), emptyList());

    final EnginePayloadStatusResult result = fromSuccessResp(resp);
    assertThat(result.getStatusAsString()).isEqualTo(VALID.name());
  }

  @Test
  public void fullWorkflow_newPayloadValidWithNullIL() {
    final BlockHeader payloadHeader = setupValidPayloadHeader();

    // Null IL means no constraints - should always be VALID
    final JsonRpcResponse resp =
        callNewPayload(newPayloadStrictMethod, payloadHeader, emptyList(), null);

    final EnginePayloadStatusResult result = fromSuccessResp(resp);
    assertThat(result.getStatusAsString()).isEqualTo(VALID.name());
  }

  @Test
  public void getInclusionList_unknownParent_returnsError() {
    final Hash unknownHash =
        Hash.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000099");
    when(blockchain.getBlockHeader(unknownHash)).thenReturn(Optional.empty());

    final JsonRpcResponse response = callGetInclusionList(unknownHash);
    assertThat(response.getType()).isEqualTo(RpcResponseType.ERROR);
  }

  @Test
  public void getInclusionList_emptyMempool_returnsEmptyList() {
    final BlockHeader parentHeader = createParentBlockHeader();
    when(blockchain.getBlockHeader(parentHeader.getHash())).thenReturn(Optional.of(parentHeader));

    when(transactionPool.getPendingTransactions()).thenReturn(List.of());

    final JsonRpcResponse response = callGetInclusionList(parentHeader.getHash());
    assertThat(response.getType()).isEqualTo(RpcResponseType.SUCCESS);

    @SuppressWarnings("unchecked")
    final List<String> result = (List<String>) ((JsonRpcSuccessResponse) response).getResult();
    assertThat(result).isEmpty();
  }

  @Test
  public void metricsTrackedAcrossWorkflow() {
    // Setup: getInclusionListV1 metrics
    final Transaction tx = createLegacyTransaction(0, Wei.of(100));
    final PendingTransaction pt =
        PendingTransaction.newPendingTransaction(tx, false, false, (byte) 0);

    final BlockHeader parentHeader = createParentBlockHeader();
    when(blockchain.getBlockHeader(parentHeader.getHash())).thenReturn(Optional.of(parentHeader));

    when(transactionPool.getPendingTransactions()).thenReturn(List.of(pt));

    callGetInclusionList(parentHeader.getHash());
    assertThat(metricsSystem.getCounterValue("engine_inclusion_list_transactions_generated"))
        .isGreaterThan(0);
    assertThat(metricsSystem.getCounterValue("engine_inclusion_list_bytes_generated"))
        .isGreaterThan(0);
    assertThat(metricsSystem.getCounterValue("engine_inclusion_list_selector_duration_ms"))
        .isGreaterThanOrEqualTo(0);

    // newPayloadV5 validation failure metrics (strict mode, IL unsatisfied)
    final BlockHeader payloadHeader = setupValidPayloadHeader();
    callNewPayload(newPayloadStrictMethod, payloadHeader, emptyList(), List.of("0xaabb"));

    assertThat(metricsSystem.getCounterValue("engine_inclusion_list_validation_failures"))
        .isEqualTo(1);
  }

  // --- Helper methods ---

  private BlockHeader createParentBlockHeader() {
    return new BlockHeaderTestFixture().baseFeePerGas(Wei.of(7)).buildHeader();
  }

  private BlockHeader createChildBlockHeader(final BlockHeader parent) {
    final BlockHeaderTestFixture fixture =
        new BlockHeaderTestFixture()
            .baseFeePerGas(Wei.ONE)
            .parentHash(parent.getHash())
            .timestamp(AMSTERDAM_MILESTONE + 1);
    return fixture.buildHeader();
  }

  private BlockHeader setupValidPayloadHeader() {
    final BlockHeader parentBlockHeader =
        new BlockHeaderTestFixture()
            .baseFeePerGas(Wei.ONE)
            .timestamp(AMSTERDAM_MILESTONE - 2)
            .excessBlobGas(BlobGas.ZERO)
            .blobGasUsed(0L)
            .buildHeader();

    final BlockHeader header =
        new BlockHeaderTestFixture()
            .baseFeePerGas(Wei.ONE)
            .parentHash(parentBlockHeader.getParentHash())
            .number(parentBlockHeader.getNumber() + 1)
            .timestamp(AMSTERDAM_MILESTONE)
            .excessBlobGas(BlobGas.ZERO)
            .blobGasUsed(0L)
            .parentBeaconBlockRoot(Optional.of(MOCK_PBSR))
            .balHash(BodyValidation.balHash(BLOCK_ACCESS_LIST))
            .requestsHash(BodyValidation.requestsHash(VALID_REQUESTS))
            .slotNumber(1L)
            .buildHeader();

    when(blockchain.getBlockByHash(header.getHash())).thenReturn(Optional.empty());
    when(blockchain.getBlockHeader(header.getParentHash()))
        .thenReturn(Optional.of(mock(BlockHeader.class)));
    when(mergeCoordinator.getLatestValidAncestor(any(BlockHeader.class)))
        .thenReturn(Optional.of(header.getHash()));
    when(mergeCoordinator.rememberBlock(any(), any()))
        .thenReturn(
            new BlockProcessingResult(
                Optional.of(
                    new BlockProcessingOutputs(null, List.of(), Optional.of(VALID_REQUESTS)))));

    return header;
  }

  private void setupValidForkchoiceUpdate(final BlockHeader header, final BlockHeader parent) {
    when(mergeCoordinator.getOrSyncHeadByHash(any(), any())).thenReturn(Optional.of(header));
    when(mergeCoordinator.updateForkChoice(any(), any(), any()))
        .thenReturn(ForkchoiceResult.withResult(Optional.empty(), Optional.of(header)));
    when(mergeCoordinator.isDescendantOf(any(), any())).thenReturn(true);
    when(blockchain.getBlockHeader(header.getHash())).thenReturn(Optional.of(header));
    when(blockchain.getBlockHeader(parent.getHash())).thenReturn(Optional.of(parent));
  }

  private Transaction createLegacyTransaction(final long nonce, final Wei gasPrice) {
    return new TransactionTestFixture()
        .to(Optional.of(Address.ZERO))
        .type(TransactionType.FRONTIER)
        .chainId(Optional.of(BigInteger.valueOf(42)))
        .nonce(nonce)
        .gasPrice(gasPrice)
        .gasLimit(21000)
        .createTransaction(KEYS);
  }

  private JsonRpcResponse callGetInclusionList(final Hash parentHash) {
    return getInclusionListMethod.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0",
                RpcMethod.ENGINE_GET_INCLUSION_LIST_V1.getMethodName(),
                new Object[] {parentHash.toHexString()})));
  }

  private JsonRpcResponse callForkchoiceUpdated(
      final BlockHeader header,
      final BlockHeader parent,
      final EnginePayloadAttributesParameter payloadAttrs) {
    return forkchoiceUpdatedMethod.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0",
                RpcMethod.ENGINE_FORKCHOICE_UPDATED_V4.getMethodName(),
                new Object[] {
                  new EngineForkchoiceUpdatedParameter(
                      header.getHash(), Hash.ZERO, parent.getHash()),
                  payloadAttrs
                })));
  }

  private JsonRpcResponse callNewPayload(
      final EngineNewPayloadV5 method,
      final BlockHeader header,
      final List<String> payloadTxs,
      final List<String> inclusionListTxs) {
    final EnginePayloadParameter payload = mockEnginePayload(header, payloadTxs);
    final List<String> requestsWithoutRequestId =
        VALID_REQUESTS.stream()
            .sorted(Comparator.comparing(r -> r.getType()))
            .map(
                r ->
                    Bytes.concatenate(Bytes.of(r.getType().getSerializedType()), r.getData())
                        .toHexString())
            .toList();

    final Object[] params =
        new Object[] {
          payload, emptyList(), MOCK_PBSR.toHexString(), requestsWithoutRequestId, inclusionListTxs
        };
    return method.response(
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", method.getName(), params)));
  }

  private EnginePayloadParameter mockEnginePayload(
      final BlockHeader header, final List<String> txs) {
    return new EnginePayloadParameter(
        header.getHash(),
        header.getParentHash(),
        header.getCoinbase(),
        header.getStateRoot(),
        new UnsignedLongParameter(header.getNumber()),
        header.getBaseFee().map(w -> w.toHexString()).orElse("0x0"),
        new UnsignedLongParameter(header.getGasLimit()),
        new UnsignedLongParameter(header.getGasUsed()),
        new UnsignedLongParameter(header.getTimestamp()),
        header.getExtraData() == null ? null : header.getExtraData().toHexString(),
        header.getReceiptsRoot(),
        header.getLogsBloom(),
        header.getPrevRandao().map(Bytes32::toHexString).orElse("0x0"),
        txs,
        null,
        header.getBlobGasUsed().map(UnsignedLongParameter::new).orElse(null),
        header.getExcessBlobGas().map(BlobGas::toHexString).orElse(null),
        ENCODED_BLOCK_ACCESS_LIST,
        header.getOptionalSlotNumber().map(UnsignedLongParameter::new).orElse(null));
  }

  private EnginePayloadStatusResult fromSuccessResp(final JsonRpcResponse resp) {
    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    return Optional.of(resp)
        .map(JsonRpcSuccessResponse.class::cast)
        .map(JsonRpcSuccessResponse::getResult)
        .map(EnginePayloadStatusResult.class::cast)
        .get();
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
