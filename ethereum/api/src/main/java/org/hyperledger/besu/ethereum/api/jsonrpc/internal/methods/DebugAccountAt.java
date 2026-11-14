/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TraceBlock.ChainUpdater;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameterOrBlockHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.BlockReplay;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor.Tracer;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ImmutableDebugAccountAtResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;
import org.hyperledger.besu.ethereum.api.query.BlockWithMetadata;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionWithMetadata;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;

public class DebugAccountAt extends AbstractBlockParameterOrBlockHashMethod {
  private final BlockReplay blockReplay;

  public DebugAccountAt(final BlockchainQueries blockchainQueries, final BlockReplay blockReplay) {
    super(blockchainQueries);
    this.blockReplay = blockReplay;
  }

  public DebugAccountAt(
      final Supplier<BlockchainQueries> blockchainQueries, final BlockReplay blockReplay) {
    super(blockchainQueries);
    this.blockReplay = blockReplay;
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_ACCOUNT_AT.getMethodName();
  }

  @Override
  protected BlockParameterOrBlockHash blockParameterOrBlockHash(
      final JsonRpcRequestContext requestContext) {
    try {
      return requestContext.getRequiredParameter(0, BlockParameterOrBlockHash.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block or block hash parameter (index 0)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }
  }

  @Override
  protected Object resultByBlockHash(
      final JsonRpcRequestContext requestContext, final Hash blockHash) {
    final Integer txIndex;
    try {
      txIndex = requestContext.getRequiredParameter(1, Integer.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid transaction index parameter (index 1)",
          RpcErrorType.INVALID_TRANSACTION_INDEX_PARAMS,
          e);
    }
    final Address address;
    try {
      address = requestContext.getRequiredParameter(2, Address.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid address parameter (index 2)", RpcErrorType.INVALID_ADDRESS_PARAMS, e);
    }

    Optional<BlockWithMetadata<TransactionWithMetadata, Hash>> block =
        blockchainQueries.get().blockByHash(blockHash);
    if (block.isEmpty()) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.BLOCK_NOT_FOUND);
    }

    List<TransactionWithMetadata> transactions = block.get().getTransactions();
    if (transactions.isEmpty() || txIndex < 0 || txIndex >= transactions.size()) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.INVALID_TRANSACTION_PARAMS);
    }

    final Hash transactionHash = transactions.get(txIndex).getTransaction().getHash();
    // A trace frame is captured after the sender nonce increment and the up-front gas debit.
    // The account has to come from the state before this transaction.
    return Tracer.processTracing(
            blockchainQueries.get(),
            Optional.of(block.get().getHeader()),
            mutableWorldState -> {
              final Optional<PriorAccount> prior =
                  accountBeforeTransaction(mutableWorldState, blockHash, transactionHash, address);
              if (prior.isEmpty()) {
                return Optional.of(
                    new JsonRpcErrorResponse(
                        requestContext.getRequest().getId(), RpcErrorType.TRANSACTION_NOT_FOUND));
              }
              final Account account = prior.get().account();
              if (account == null || !account.getAddress().equals(address)) {
                return Optional.of(
                    new JsonRpcErrorResponse(
                        requestContext.getRequest().getId(), RpcErrorType.NO_ACCOUNT_FOUND));
              }
              return Optional.of(
                  debugAccountAtResult(
                      account.getCode(),
                      Quantity.create(account.getNonce()),
                      Quantity.create(account.getBalance()),
                      Quantity.create(account.getCodeHash().getBytes())));
            })
        .orElse(
            new JsonRpcErrorResponse(
                requestContext.getRequest().getId(), RpcErrorType.WORLD_STATE_UNAVAILABLE));
  }

  /**
   * Replays the transactions before {@code transactionHash} and reads {@code address} from the
   * resulting state. The selected transaction itself is not applied. An empty result means that
   * hash is not in the block.
   */
  private Optional<PriorAccount> accountBeforeTransaction(
      final MutableWorldState mutableWorldState,
      final Hash blockHash,
      final Hash transactionHash,
      final Address address) {
    return blockReplay.performActionWithBlock(
        blockHash,
        (block, blockchain, transactionProcessor, protocolSpec) -> {
          final ChainUpdater chainUpdater = new ChainUpdater(mutableWorldState);
          WorldUpdater stateBeforeTransaction = null;
          Wei blobGasPrice = null;
          BlockHashLookup blockHashLookup = null;
          for (final Transaction transaction : block.getBody().getTransactions()) {
            if (transaction.getHash().equals(transactionHash)) {
              final Account account =
                  stateBeforeTransaction == null
                      ? mutableWorldState.get(address)
                      : stateBeforeTransaction.get(address);
              return Optional.of(new PriorAccount(account));
            }
            if (blockHashLookup == null) {
              final BlockHeader header = block.getHeader();
              blobGasPrice =
                  protocolSpec
                      .getFeeMarket()
                      .blobGasPricePerGas(
                          blockchain
                              .getBlockHeader(header.getParentHash())
                              .map(parent -> calculateExcessBlobGasForParent(protocolSpec, parent))
                              .orElse(BlobGas.ZERO));
              blockHashLookup =
                  protocolSpec.getPreExecutionProcessor().createBlockHashLookup(blockchain, header);
            }
            final WorldUpdater updater = chainUpdater.getNextUpdater();
            final TransactionProcessingResult result =
                transactionProcessor.processTransaction(
                    updater,
                    block.getHeader(),
                    transaction,
                    protocolSpec
                        .getMiningBeneficiaryCalculator()
                        .calculateBeneficiary(block.getHeader()),
                    blockHashLookup,
                    TransactionValidationParams.blockReplay(),
                    blobGasPrice);
            // A mined transaction is valid at its position, so an invalid result is a replay
            // failure. A reverted transaction still changes the nonce and the balance.
            if (result.isInvalid()) {
              throw new IllegalStateException(
                  "Transaction replay failed: " + result.getValidationResult().getErrorMessage());
            }
            stateBeforeTransaction = updater;
          }
          return Optional.empty();
        });
  }

  protected ImmutableDebugAccountAtResult debugAccountAtResult(
      final Bytes code, final String nonce, final String balance, final String codeHash) {
    return ImmutableDebugAccountAtResult.builder()
        .code(code.isEmpty() ? "0x0" : code.toHexString())
        .nonce(nonce)
        .balance(balance)
        .codehash(codeHash)
        .build();
  }

  /** The account at the address before the transaction, or null when the account does not exist. */
  private record PriorAccount(Account account) {}
}
