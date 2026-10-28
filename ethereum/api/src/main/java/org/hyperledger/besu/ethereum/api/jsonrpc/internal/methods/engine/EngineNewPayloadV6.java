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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.PayloadStatusV2;
import org.hyperledger.besu.ethereum.chain.InclusionListStatus;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.inclusionlist.InclusionListTransactions;
import org.hyperledger.besu.ethereum.mainnet.inclusionlist.InclusionListValidationResult;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
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

  private final MutableBlockchain blockchain;

  public EngineNewPayloadV6(
      final ConstructorArguments constructorArguments,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(constructorArguments, minSupportedFork, firstUnsupportedFork);
    this.blockchain = constructorArguments.protocolContext().getBlockchain();
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

    final InclusionListTransactions inclusionListTransactions;
    try {
      // deduplicated and sorted by hash by InclusionListTransactions
      inclusionListTransactions =
          new InclusionListTransactions(
              requestContext.getRequiredLenientList(
                  4, Transaction.class, Configuration.FAIL_ON_UNKNOWN_BUT_NULL));
      logger()
          .info(
              "Received {} unique inclusion list transactions",
              inclusionListTransactions.getTransactions().size());
    } catch (JsonRpcParameterException e) {
      throw new InvalidRequestParametersException(
          requestParameters.payloadParameter(),
          "Invalid inclusion list transactions parameters (index 4)",
          RpcErrorType.INVALID_INCLUSION_LIST_TRANSACTIONS_PARAMS,
          e);
    }

    return (NPRP) new NewPayloadRequestParametersV4<>(requestParameters, inclusionListTransactions);
  }

  /**
   * The inclusion list can only be validated while processing the block, so a payload already
   * present is only answered without processing it again if it was validated against the same
   * inclusion list, otherwise it is processed again with the inclusion list of this request.
   */
  @Override
  protected boolean isPayloadPresent(final Block block, final NPRP requestParameters) {
    final Bytes32 inclusionListChecksum =
        requestParameters.inclusionListTransactions().getChecksum();
    return super.isPayloadPresent(block, requestParameters)
        && blockchain
            .getInclusionListStatus(block.getHash())
            .filter(status -> status.isFor(inclusionListChecksum))
            .isPresent();
  }

  @Override
  protected PayloadStatusV2 createValidPayloadStatus(
      final Hash latestValidHash,
      final BlockProcessingResult executionResult,
      final NPRP requestParameters) {
    final boolean ilSatisfied;
    if (executionResult == null) {
      // already present payload: isPayloadPresent ensures its status was stored for the inclusion
      // list of this request
      ilSatisfied =
          blockchain
              .getInclusionListStatus(latestValidHash)
              .map(InclusionListStatus::satisfied)
              .orElse(true);
    } else {
      ilSatisfied =
          executionResult
              .getYield()
              .map(BlockProcessingOutputs::getInclusionListValidationResult)
              .map(InclusionListValidationResult::isValid)
              .orElse(true);
      blockchain.putInclusionListStatus(
          latestValidHash,
          new InclusionListStatus(
              requestParameters.inclusionListTransactions().getChecksum(), ilSatisfied));
    }

    return new PayloadStatusV2(VALID, latestValidHash, ilSatisfied);
  }

  @Override
  protected PayloadStatusV2 createInvalidPayloadStatus(
      final EngineStatus invalidStatus, final Hash latestValidHash, final String validationError) {
    return new PayloadStatusV2(invalidStatus, latestValidHash, validationError);
  }

  @Override
  protected PayloadStatusV2 createSyncingPayloadStatus() {
    return new PayloadStatusV2(SYNCING);
  }

  @Override
  protected BlockProcessingResult rememberBlock(final Block block, final NPRP requestParameters) {
    return mergeCoordinator.rememberBlock(
        block,
        Optional.of(requestParameters.payloadParameter().getBlockAccessList()),
        requestParameters.inclusionListTransactions().getTransactions(),
        writeAfterReply(block));
  }
}
