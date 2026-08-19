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

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.ACCEPTED;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.SYNCING;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.Configuration;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.PayloadPostExecutionValidationResultV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.PayloadStatusV2;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;
import org.hyperledger.besu.ethereum.eth.transactions.inclusionlist.InclusionListValidationResult;
import org.hyperledger.besu.ethereum.eth.transactions.inclusionlist.InclusionListValidator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code engine_newPayloadV6} — Bogotà (EIP-7805 Inclusion Lists).
 *
 * <p>Extends V5 with a mandatory 5th parameter carrying the inclusion-list transactions (as opaque
 * RLP hex strings) that the proposer committed to. After the payload is successfully processed,
 * verifies that every inclusion-list transaction is either present in the block or could not have
 * been included, responding with {@code INCLUSION_LIST_UNSATISFIED} otherwise.
 */
public final class EngineNewPayloadV6<
        EP extends ExecutionPayloadV4, NPRP extends NewPayloadRequestParametersV4<? extends EP>>
    extends EngineNewPayloadV5<EP, NPRP> {

  private static final Logger LOG = LoggerFactory.getLogger(EngineNewPayloadV6.class);
  private final InclusionListValidator inclusionListValidator = new InclusionListValidator();

  public EngineNewPayloadV6(
      final ConstructorArguments constructorArguments,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(constructorArguments, minSupportedFork, firstUnsupportedFork);
  }

  @Override
  protected Logger logger() {
    return LOG;
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_NEW_PAYLOAD_V6.getMethodName();
  }

  @Override
  protected int getNumberOfParameters() {
    return 5;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected NPRP readRequestParameters(final JsonRpcRequestContext requestContext) {
    final NewPayloadRequestParametersV3<? extends EP> requestParameters =
        super.readRequestParameters(requestContext);

    final List<String> inclusionListTransactions;
    try {
      inclusionListTransactions =
          requestContext.getRequiredList(4, String.class, Configuration.FAIL_ON_UNKNOWN_BUT_NULL);
    } catch (JsonRpcParameterException e) {
      throw new InvalidRequestParametersException(
          requestParameters.payloadParameter(),
          "Invalid inclusion list transactions parameters (index 4)",
          RpcErrorType.INVALID_INCLUSION_LIST_TRANSACTIONS_PARAMS,
          e);
    }

    return (NPRP) new NewPayloadRequestParametersV4<>(requestParameters, inclusionListTransactions);
  }

  @Override
  protected void processAcceptedBlock(final Block block, final NPRP requestParameters) {
    protocolContext
        .getBlockchain()
        .storeInclusionListTransactions(
            block.getHash(), requestParameters.inclusionListTransactions());
  }

  @Override
  protected PayloadStatusV2 createValidPayloadStatus(
      final Hash latestValidHash,
      final PayloadPostExecutionValidationResultV1 postExecutionResult) {
    return new PayloadStatusV2(
        VALID, latestValidHash, postExecutionResult.isInclusionListSatisfied());
  }

  @Override
  protected PayloadStatusV2 createInvalidPayloadStatus(
      final EngineStatus invalidStatus, final Hash latestValidHash, final String validationError) {
    return new PayloadStatusV2(invalidStatus, latestValidHash, validationError);
  }

  @Override
  protected PayloadStatusV2 createAcceptedPayloadStatus() {
    return new PayloadStatusV2(ACCEPTED);
  }

  @Override
  protected PayloadStatusV2 createSyncingPayloadStatus() {
    return new PayloadStatusV2(SYNCING);
  }

  @Override
  protected PayloadPostExecutionValidationResultV1 validatePostExecution(
      final Object reqId,
      final NPRP requestParameters,
      final Block block,
      final BlockProcessingResult executionResult) {

    final EP blockParam = requestParameters.payloadParameter();
    final List<String> inclusionListHexTransactions = requestParameters.inclusionListTransactions();
    if (inclusionListHexTransactions == null || inclusionListHexTransactions.isEmpty()) {
      return PayloadPostExecutionValidationResultV1.SUCCESS;
    }

    try {
      final Set<Transaction> payloadTransactions = Set.copyOf(blockParam.getTransactions());
      final List<Bytes> notConfirmedILTxs = new ArrayList<>();
      for (final String ilHexTx : inclusionListHexTransactions) {
        final Bytes rawTx = Bytes.fromHexString(ilHexTx);
        if (isAlreadyInPayload(rawTx, payloadTransactions)) {
          LOG.info("IL tx already confirmed: {}", ilHexTx);
        } else {
          LOG.info("IL tx not confirmed, verify if it could be included: {}", ilHexTx);
          notConfirmedILTxs.add(rawTx);
        }
      }

      if (notConfirmedILTxs.isEmpty()) {
        return PayloadPostExecutionValidationResultV1.SUCCESS;
      }

      final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(block.getHeader());

      final BlockProcessingOutputs blockProcessingOutputs =
          executionResult
              .getYield()
              .orElseThrow(() -> new IllegalStateException("No block processing outputs present"));

      final InclusionListValidationResult result =
          inclusionListValidator.validate(
              protocolSpec,
              protocolContext,
              block.getHeader(),
              blockProcessingOutputs.getCumulativeBlockGasUsed(),
              notConfirmedILTxs);
      if (result.isValid()) {
        return PayloadPostExecutionValidationResultV1.SUCCESS;
      }

      return new PayloadPostExecutionValidationResultV1(false);
    } catch (final Exception e) {
      throw e;
    }
  }

  private boolean isAlreadyInPayload(
      final Bytes rawTx, final Set<Transaction> payloadTransactions) {
    try {
      return payloadTransactions.contains(
          TransactionDecoder.decodeOpaqueBytes(rawTx, EncodingContext.BLOCK_BODY));
    } catch (final Exception e) {
      // undecodable transactions cannot be confirmed as already included; let the validator
      // decide (it safely skips undecodable bytes too)
      return false;
    }
  }
}
