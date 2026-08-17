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

import org.hyperledger.besu.consensus.merge.blockcreation.PreparePayloadArgsBuilder;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.PayloadAttributesV5;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code engine_forkchoiceUpdatedV5} — Bogotà (EIP-7805 Inclusion Lists).
 *
 * <p>Extends V4 with {@link PayloadAttributesV5}, adding the mandatory {@code
 * inclusionListTransactions} field. Decoded inclusion-list transactions are added to the
 * transaction pool and forwarded to {@code preparePayload} so block building can satisfy them.
 */
public final class EngineForkchoiceUpdatedV5 extends EngineForkchoiceUpdatedV4<PayloadAttributesV5> {

  private static final Logger LOG = LoggerFactory.getLogger(EngineForkchoiceUpdatedV5.class);

  private final TransactionPool transactionPool;

  @Override
  protected Logger logger() {
    return LOG;
  }

  public EngineForkchoiceUpdatedV5(
      final ConstructorArguments constructorArguments,
      final HardforkId minFork,
      final HardforkId maxFork) {
    super(constructorArguments, minFork, maxFork);
    this.transactionPool = constructorArguments.transactionPool().orElseThrow();
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_FORKCHOICE_UPDATED_V5.getMethodName();
  }

  @Override
  protected Class<PayloadAttributesV5> getPayloadAttributesClass() {
    return PayloadAttributesV5.class;
  }

  /**
   * V5 requires {@code inclusionListTransactions} in addition to everything V4 requires. Delegates
   * to V4 first (which checks {@code slotNumber} and {@code targetGasLimit}), then adds its own
   * check.
   */
  @Override
  protected ValidationResult<RpcErrorType> validatePayloadAttributes(
      final BlockHeader newHead, final PayloadAttributesV5 attrs) {
    final ValidationResult<RpcErrorType> r = super.validatePayloadAttributes(newHead, attrs);
    return r.isValid() ? validatePayloadAttributesV5(attrs) : r;
  }

  private ValidationResult<RpcErrorType> validatePayloadAttributesV5(
      final PayloadAttributesV5 attrs) {
    if (attrs.getInclusionListTransactions() == null) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_INCLUSION_LIST_TRANSACTIONS_PARAMS,
          "Missing inclusionListTransactions");
    }
    return ValidationResult.valid();
  }

  @Override
  protected void setPreparePayloadArgs(
      final PreparePayloadArgsBuilder preparePayloadArgsBuilder, final PayloadAttributesV5 attrs) {
    super.setPreparePayloadArgs(preparePayloadArgsBuilder, attrs);
    final List<Transaction> inclusionListTransactions =
        decodeInclusionListTransactions(attrs.getInclusionListTransactions());
    final var ilTxsAddedResult = transactionPool.addRemoteTransactions(inclusionListTransactions);
    logger().trace("Inclusion list transactions added to txpool, result {}", ilTxsAddedResult);
    preparePayloadArgsBuilder.inclusionListTransactions(inclusionListTransactions);
  }

  private List<Transaction> decodeInclusionListTransactions(final List<Bytes> rawTransactions) {
    if (rawTransactions == null || rawTransactions.isEmpty()) {
      return List.of();
    }

    final List<Transaction> ilTxs = new ArrayList<>(rawTransactions.size());
    for (final Bytes rawTransaction : rawTransactions) {
      try {
        ilTxs.add(TransactionDecoder.decodeOpaqueBytes(rawTransaction, EncodingContext.BLOCK_BODY));
      } catch (final Exception e) {
        logger()
            .atInfo()
            .setMessage("Ignoring invalid IL tx bytes {}")
            .addArgument(rawTransaction::toHexString)
            .log();
      }
    }

    logger()
        .atInfo()
        .setMessage("Received {} inclusion list transactions")
        .addArgument(ilTxs.size())
        .log();
    return ilTxs;
  }
}
