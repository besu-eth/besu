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

import static com.google.common.base.Preconditions.checkState;
import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.datatypes.parameters.UnsignedIntParameter;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.FeeHistory;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ImmutableFeeHistory;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.util.cache.MemoryBoundCache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.units.bigints.UInt256s;

public class EthFeeHistory implements JsonRpcMethod {
  private final ProtocolSchedule protocolSchedule;
  private final BlockchainQueries blockchainQueries;
  private final Blockchain blockchain;
  private final MiningCoordinator miningCoordinator;
  private final ApiConfiguration apiConfiguration;
  private final MemoryBoundCache<RewardCacheKey, List<Wei>> perBlockRewardsCache;
  // Full results for "latest" requests; historical queries are not cached.
  private final MemoryBoundCache<ResultCacheKey, FeeHistory.FeeHistoryResult> resultCache;
  // Entry size varies with percentile count and block range, so both caches are byte-bounded.
  private static final long PER_BLOCK_REWARDS_CACHE_MAX_BYTES = 32L * 1024 * 1024;
  private static final long RESULT_CACHE_MAX_BYTES = 16L * 1024 * 1024;
  private static final int MAXIMUM_QUERY_PERCENTILES = 100;

  record RewardCacheKey(Hash blockHash, List<Double> rewardPercentiles) {}

  // Per-request mining-config snapshot; part of the cache key so a mid-flight
  // miner_setMinGasPrice / miner_setMinPriorityFee can't leak a stale bounded result.
  record RewardBounds(Wei lowerBoundGasPrice, Wei minPriorityFee) {}

  record ResultCacheKey(
      Hash chainHeadHash,
      int blockCount,
      Optional<List<Double>> sortedRewardPercentiles,
      Optional<RewardBounds> rewardBounds,
      HardforkId nextBlockHardforkId) {}

  private static int perBlockRewardsEntryWeight(final RewardCacheKey key, final List<Wei> rewards) {
    return 128 + 32 * key.rewardPercentiles().size() + 80 * rewards.size();
  }

  private static int resultEntryWeight(
      final ResultCacheKey key, final FeeHistory.FeeHistoryResult result) {
    int weight =
        256
            + 32 * key.sortedRewardPercentiles().map(List::size).orElse(0)
            + 32 * (result.getGasUsedRatio().size() + result.getBlobGasUsedRatio().size())
            + 64 * (result.getBaseFeePerGas().size() + result.getBaseFeePerBlobGas().size());
    final List<List<String>> reward = result.getReward();
    if (reward != null) {
      for (final List<String> blockRewards : reward) {
        weight += 64 + 64 * blockRewards.size();
      }
    }
    return weight;
  }

  public EthFeeHistory(
      final ProtocolSchedule protocolSchedule,
      final BlockchainQueries blockchainQueries,
      final MiningCoordinator miningCoordinator,
      final ApiConfiguration apiConfiguration) {
    this.protocolSchedule = protocolSchedule;
    this.blockchainQueries = blockchainQueries;
    this.miningCoordinator = miningCoordinator;
    this.apiConfiguration = apiConfiguration;
    this.blockchain = blockchainQueries.getBlockchain();
    this.perBlockRewardsCache =
        new MemoryBoundCache<>(
            PER_BLOCK_REWARDS_CACHE_MAX_BYTES, EthFeeHistory::perBlockRewardsEntryWeight);
    this.resultCache =
        new MemoryBoundCache<>(RESULT_CACHE_MAX_BYTES, EthFeeHistory::resultEntryWeight);
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_FEE_HISTORY.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext request) {
    final Object requestId = request.getRequest().getId();

    final int blockCount;
    try {
      blockCount = request.getRequiredParameter(0, UnsignedIntParameter.class).getValue();
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block count parameter (index 0)", RpcErrorType.INVALID_BLOCK_COUNT_PARAMS, e);
    }
    if (isInvalidBlockCount(blockCount)) {
      return new JsonRpcErrorResponse(requestId, RpcErrorType.INVALID_BLOCK_COUNT_PARAMS);
    }
    final BlockParameter highestBlock;
    try {
      highestBlock = request.getRequiredParameter(1, BlockParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid highest block parameter (index 1)", RpcErrorType.INVALID_BLOCK_PARAMS, e);
    }

    final Optional<List<Double>> maybeRewardPercentiles;
    try {
      maybeRewardPercentiles = request.getOptionalParameter(2, Double[].class).map(Arrays::asList);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid reward percentiles parameter (index 2)",
          RpcErrorType.INVALID_REWARD_PERCENTILES_PARAMS,
          e);
    }

    final BlockHeader chainHeadHeader = blockchain.getChainHeadHeader();
    final long chainHeadBlockNumber = chainHeadHeader.getNumber();
    final long highestBlockNumber = highestBlock.getNumber().orElse(chainHeadBlockNumber);
    if (highestBlockNumber > chainHeadBlockNumber) {
      return new JsonRpcErrorResponse(requestId, RpcErrorType.INVALID_BLOCK_NUMBER_PARAMS);
    }

    final boolean isLatestRequest = highestBlockNumber == chainHeadBlockNumber;
    final Optional<RewardBounds> rewardBounds =
        maybeRewardPercentiles.isPresent() && apiConfiguration.isGasAndPriorityFeeLimitingEnabled()
            ? Optional.of(
                new RewardBounds(
                    blockchainQueries.gasPriceLowerBound(),
                    miningCoordinator.getMinPriorityFeePerGas()))
            : Optional.empty();
    final ProtocolSpec nextBlockProtocolSpec =
        protocolSchedule.getForNextBlockHeader(chainHeadHeader, chainHeadHeader.getTimestamp());
    final Optional<List<Double>> sortedRewardPercentiles =
        maybeRewardPercentiles
            .filter(list -> list.size() <= MAXIMUM_QUERY_PERCENTILES)
            .map(percentiles -> percentiles.stream().sorted().toList());
    final ResultCacheKey resultCacheKey =
        isLatestRequest
            ? new ResultCacheKey(
                chainHeadHeader.getBlockHash(),
                blockCount,
                sortedRewardPercentiles,
                rewardBounds,
                nextBlockProtocolSpec.getHardforkId())
            : null;

    if (resultCacheKey != null) {
      final FeeHistory.FeeHistoryResult cached = resultCache.getIfPresent(resultCacheKey);
      if (cached != null) {
        return new JsonRpcSuccessResponse(requestId, cached);
      }
    }

    final long firstBlock = Math.max(0, highestBlockNumber - (blockCount - 1));
    final long lastBlock =
        blockCount > highestBlockNumber ? (highestBlockNumber + 1) : (firstBlock + blockCount);

    final List<BlockHeader> blockHeaderRange = getBlockHeaders(firstBlock, lastBlock);
    final List<Wei> requestedBaseFees = getBaseFees(blockHeaderRange);
    final List<Wei> requestedBlobBaseFees =
        getBlobBaseFees(blockHeaderRange, nextBlockProtocolSpec);
    final Wei nextBaseFee =
        getNextBaseFee(highestBlockNumber, chainHeadHeader, requestedBaseFees, blockHeaderRange);
    final List<Double> gasUsedRatios = getGasUsedRatios(blockHeaderRange);
    final List<Double> blobGasUsedRatios = getBlobGasUsedRatios(blockHeaderRange);
    final Optional<List<List<Wei>>> maybeRewards =
        sortedRewardPercentiles.map(
            percentiles ->
                computeRewardsForRange(percentiles, blockHeaderRange, nextBaseFee, rewardBounds));
    final FeeHistory.FeeHistoryResult result =
        createFeeHistoryResult(
            firstBlock,
            requestedBaseFees,
            requestedBlobBaseFees,
            nextBaseFee,
            gasUsedRatios,
            blobGasUsedRatios,
            maybeRewards);
    if (resultCacheKey != null) {
      resultCache.put(resultCacheKey, result);
    }
    return new JsonRpcSuccessResponse(requestId, result);
  }

  private List<List<Wei>> computeRewardsForRange(
      final List<Double> sortedPercentiles,
      final List<BlockHeader> blockHeaders,
      final Wei nextBaseFee,
      final Optional<RewardBounds> rewardBounds) {
    final List<List<Wei>> rewards = new ArrayList<>(blockHeaders.size());
    if (rewardBounds.isEmpty()) {
      for (final BlockHeader header : blockHeaders) {
        calculateBlockHeaderReward(sortedPercentiles, header).ifPresent(rewards::add);
      }
      return rewards;
    }
    final RewardBounds bounds = rewardBounds.get();
    for (final BlockHeader header : blockHeaders) {
      calculateBlockHeaderReward(sortedPercentiles, header)
          .map(blockRewards -> boundRewardsWithSnapshot(blockRewards, nextBaseFee, bounds))
          .ifPresent(rewards::add);
    }
    return rewards;
  }

  private Wei getNextBaseFee(
      final long resolvedHighestBlockNumber,
      final BlockHeader chainHeadHeader,
      final List<Wei> explicitlyRequestedBaseFees,
      final List<BlockHeader> blockHeaders) {
    final long nextBlockNumber = resolvedHighestBlockNumber + 1;
    // For "latest" (the common case) nextBlockNumber > chainHead, so the block doesn't exist yet:
    // compute directly and skip the storage read. Only historical-block queries need the lookup.
    if (nextBlockNumber > chainHeadHeader.getNumber()) {
      return computeNextBaseFee(
          nextBlockNumber, chainHeadHeader, explicitlyRequestedBaseFees, blockHeaders);
    }
    return blockchain
        .getBlockHeader(nextBlockNumber)
        .map(blockHeader -> blockHeader.getBaseFee().orElse(Wei.ZERO))
        .orElseGet(
            () ->
                computeNextBaseFee(
                    nextBlockNumber, chainHeadHeader, explicitlyRequestedBaseFees, blockHeaders));
  }

  private Wei computeNextBaseFee(
      final long nextBlockNumber,
      final BlockHeader chainHeadHeader,
      final List<Wei> explicitlyRequestedBaseFees,
      final List<BlockHeader> blockHeaders) {

    // Note: We are able to use the chain head timestamp for next block header as
    // the base fee market can only be pre or post London. If another fee
    // market is added, we will need to reconsider this.

    // Get the fee market for the next block header
    Optional<FeeMarket> feeMarketOptional =
        Optional.of(
            protocolSchedule
                .getForNextBlockHeader(chainHeadHeader, chainHeadHeader.getTimestamp())
                .getFeeMarket());

    // If the fee market implements base fee, compute the next base fee
    return feeMarketOptional
        .filter(FeeMarket::implementsBaseFee)
        .map(BaseFeeMarket.class::cast)
        .map(
            feeMarket -> {
              // Get the last block header and the last explicitly requested base fee
              final BlockHeader lastBlockHeader = blockHeaders.get(blockHeaders.size() - 1);
              final Wei lastExplicitlyRequestedBaseFee =
                  explicitlyRequestedBaseFees.get(explicitlyRequestedBaseFees.size() - 1);

              // Compute the next base fee
              return feeMarket.computeBaseFee(
                  nextBlockNumber,
                  lastExplicitlyRequestedBaseFee,
                  lastBlockHeader.getGasUsed(),
                  feeMarket.targetGasUsed(lastBlockHeader));
            })
        .orElse(Wei.ZERO); // If the fee market does not implement base fee, return zero
  }

  private Optional<List<Wei>> calculateBlockHeaderReward(
      final List<Double> sortedPercentiles, final BlockHeader blockHeader) {

    // Cache only unbounded rewards (a pure function of immutable block state); the caller applies
    // request-specific bounds via boundRewardsWithSnapshot, so miner-config changes need no
    // invalidation.
    final RewardCacheKey key = new RewardCacheKey(blockHeader.getBlockHash(), sortedPercentiles);

    return Optional.ofNullable(perBlockRewardsCache.getIfPresent(key))
        .or(
            () -> {
              final Optional<Block> block = blockchain.getBlockByHash(blockHeader.getBlockHash());
              return block.map(
                  b -> {
                    final List<Wei> rewards = computeUnboundedRewards(sortedPercentiles, b);
                    perBlockRewardsCache.put(key, rewards);
                    return rewards;
                  });
            });
  }

  record TransactionInfo(long gasUsed, Wei effectivePriorityFeePerGas) {}

  @VisibleForTesting
  public List<Wei> computeRewards(
      final List<Double> rewardPercentiles, final Block block, final Wei nextBaseFee) {
    final List<Wei> realRewards = computeUnboundedRewards(rewardPercentiles, block);
    if (apiConfiguration.isGasAndPriorityFeeLimitingEnabled()) {
      return boundRewardsWithSnapshot(
          realRewards,
          nextBaseFee,
          new RewardBounds(
              blockchainQueries.gasPriceLowerBound(), miningCoordinator.getMinPriorityFeePerGas()));
    }
    return realRewards;
  }

  private List<Wei> computeUnboundedRewards(
      final List<Double> rewardPercentiles, final Block block) {
    final List<Transaction> transactions = block.getBody().getTransactions();
    if (transactions.isEmpty()) {
      return Collections.nCopies(rewardPercentiles.size(), Wei.ZERO);
    }
    final Optional<Wei> baseFee = block.getHeader().getBaseFee();
    final long[] transactionsGasUsed = calculateTransactionsGasUsed(block);
    // Receipt count = transaction count is guaranteed by the header's receipts root; a mismatch
    // means corrupted local storage, so fail loudly rather than truncate to the shorter list.
    checkState(
        transactionsGasUsed.length == transactions.size(),
        "Block %s has %s transactions but %s receipts: receipts/body storage mismatch",
        block.getHash(),
        transactions.size(),
        transactionsGasUsed.length);
    final TransactionInfo[] transactionsInfo =
        generateTransactionsInfo(transactions, transactionsGasUsed, baseFee);
    return calculateRewards(rewardPercentiles, block, transactionsInfo);
  }

  private List<Wei> calculateRewards(
      final List<Double> rewardPercentiles,
      final Block block,
      final TransactionInfo[] sortedTransactionsInfo) {
    final ArrayList<Wei> rewards = new ArrayList<>(rewardPercentiles.size());

    // Start with the gas used by the first transaction
    double cumulativeGasUsed = sortedTransactionsInfo[0].gasUsed();
    int transactionIndex = 0;
    final long blockGasUsed = block.getHeader().getGasUsed();
    // Iterate over each reward percentile
    for (double rewardPercentile : rewardPercentiles) {
      // Calculate the threshold gas used for the current reward percentile
      // This is the amount of gas that needs to be used to reach this percentile
      final double thresholdGasUsed = rewardPercentile * blockGasUsed / 100;

      // Update cumulativeGasUsed by adding the gas used by each transaction
      // Stop when cumulativeGasUsed reaches the threshold or there are no more transactions
      while (cumulativeGasUsed < thresholdGasUsed
          && transactionIndex < sortedTransactionsInfo.length - 1) {
        transactionIndex++;
        cumulativeGasUsed += sortedTransactionsInfo[transactionIndex].gasUsed();
      }
      // Add the effective priority fee per gas of the transaction that reached the percentile to
      // the rewards list
      rewards.add(sortedTransactionsInfo[transactionIndex].effectivePriorityFeePerGas);
    }
    return rewards;
  }

  /**
   * Bound each reward to a min/max derived from the supplied mining-config snapshot. The snapshot
   * is taken once per request and threaded through here so the cache key carries the exact values
   * used in computation (avoids a torn read when miner_setMinGasPrice / miner_setMinPriorityFee
   * lands mid-request).
   */
  private List<Wei> boundRewardsWithSnapshot(
      final List<Wei> rewards, final Wei nextBaseFee, final RewardBounds bounds) {
    final Wei lowerBoundPriorityFee =
        bounds.lowerBoundGasPrice().greaterThan(nextBaseFee)
            ? bounds.lowerBoundGasPrice().subtract(nextBaseFee)
            : Wei.ZERO;
    final Wei forcedMinPriorityFee = UInt256s.max(bounds.minPriorityFee(), lowerBoundPriorityFee);
    final Wei lowerBound =
        forcedMinPriorityFee
            .multiply(apiConfiguration.getLowerBoundGasAndPriorityFeeCoefficient())
            .divide(100);
    final Wei upperBound =
        forcedMinPriorityFee
            .multiply(apiConfiguration.getUpperBoundGasAndPriorityFeeCoefficient())
            .divide(100);

    final List<Wei> bounded = new ArrayList<>(rewards.size());
    for (final Wei reward : rewards) {
      bounded.add(boundReward(reward, lowerBound, upperBound));
    }
    return bounded;
  }

  /**
   * This method bounds the reward between a lower and upper limit.
   *
   * @param reward The reward to be bounded.
   * @param lowerBound The lower limit for the reward.
   * @param upperBound The upper limit for the reward.
   * @return The bounded reward.
   */
  private Wei boundReward(final Wei reward, final Wei lowerBound, final Wei upperBound) {
    return reward.compareTo(lowerBound) <= 0
        ? lowerBound
        : reward.compareTo(upperBound) >= 0 ? upperBound : reward;
  }

  private long[] calculateTransactionsGasUsed(final Block block) {
    final List<TransactionReceipt> receipts = blockchain.getTxReceipts(block.getHash()).get();
    final long[] gasUsed = new long[receipts.size()];
    long cumulativeGasUsed = 0L;
    for (int i = 0; i < receipts.size(); i++) {
      final long cum = receipts.get(i).getCumulativeGasUsed();
      gasUsed[i] = cum - cumulativeGasUsed;
      cumulativeGasUsed = cum;
    }
    return gasUsed;
  }

  private TransactionInfo[] generateTransactionsInfo(
      final List<Transaction> transactions,
      final long[] transactionsGasUsed,
      final Optional<Wei> baseFee) {
    // Direct array + comparator instead of Streams.zip + Comparator.comparing: avoids
    // stream-pipeline boxing on the hot path for large reward-percentile ranges.
    final int n = transactions.size();
    final TransactionInfo[] info = new TransactionInfo[n];
    for (int i = 0; i < n; i++) {
      final Transaction tx = transactions.get(i);
      info[i] =
          new TransactionInfo(transactionsGasUsed[i], tx.getEffectivePriorityFeePerGas(baseFee));
    }
    Arrays.sort(
        info, (a, b) -> a.effectivePriorityFeePerGas().compareTo(b.effectivePriorityFeePerGas()));
    return info;
  }

  private boolean isInvalidBlockCount(final int blockCount) {
    return blockCount < 1 || blockCount > 1024;
  }

  private List<BlockHeader> getBlockHeaders(final long oldestBlock, final long lastBlock) {
    final int count = (int) (lastBlock - oldestBlock);
    if (count <= 0) {
      return List.of();
    }
    return blockchain.getBlockHeaders(oldestBlock, count);
  }

  private List<Wei> getBaseFees(final List<BlockHeader> blockHeaders) {
    return blockHeaders.stream()
        .map(blockHeader -> blockHeader.getBaseFee().orElse(Wei.ZERO))
        .toList();
  }

  private List<Wei> getBlobBaseFees(
      final List<BlockHeader> blockHeaders, final ProtocolSpec nextBlockProtocolSpec) {
    if (blockHeaders.isEmpty()) {
      return Collections.emptyList();
    }
    // Headers are sorted oldest→newest, so blockHeaders[i] is the parent of blockHeaders[i+1].
    // Reuse it instead of a per-block parent-hash storage read; only the first header needs a
    // real parent lookup.
    final Wei[] baseFeesPerBlobGas = new Wei[blockHeaders.size() + 1];
    BlockHeader previous = null;
    int i = 0;
    for (; i < blockHeaders.size(); i++) {
      final BlockHeader header = blockHeaders.get(i);
      final BlockHeader parent;
      if (previous != null && header.getParentHash().equals(previous.getHash())) {
        parent = previous;
      } else {
        parent = blockchain.getBlockHeader(header.getParentHash()).orElse(null);
      }
      if (parent == null) {
        baseFeesPerBlobGas[i] = Wei.ZERO;
      } else {
        baseFeesPerBlobGas[i] = getBlobGasFee(protocolSchedule.getByBlockHeader(header), parent);
      }
      previous = header;
    }

    baseFeesPerBlobGas[i] = getNextBlobFee(blockHeaders.getLast(), nextBlockProtocolSpec);
    return Arrays.asList(baseFeesPerBlobGas);
  }

  private Wei getBlobGasFee(final ProtocolSpec spec, final BlockHeader parent) {
    return spec.getFeeMarket().blobGasPricePerGas(calculateExcessBlobGasForParent(spec, parent));
  }

  private Wei getNextBlobFee(final BlockHeader header, final ProtocolSpec nextBlockProtocolSpec) {
    // Attempt to retrieve the next header based on the current header's number.
    final long nextBlockNumber = header.getNumber() + 1;
    // Skip the storage lookup if we already know the next block doesn't exist — true for
    // every "latest" request, which is the dominant case for feeHistory.
    if (nextBlockNumber > blockchain.getChainHeadBlockNumber()) {
      return getBlobGasFee(nextBlockProtocolSpec, header);
    }
    return blockchain
        .getBlockHeader(nextBlockNumber)
        .map(nextHeader -> getBlobGasFee(protocolSchedule.getByBlockHeader(nextHeader), header))
        .orElseGet(
            () ->
                getBlobGasFee(
                    protocolSchedule.getForNextBlockHeader(header, header.getTimestamp()), header));
  }

  private List<Double> getGasUsedRatios(final List<BlockHeader> blockHeaders) {
    return blockHeaders.stream()
        .map(blockHeader -> blockHeader.getGasUsed() / (double) blockHeader.getGasLimit())
        .toList();
  }

  private List<Double> getBlobGasUsedRatios(final List<BlockHeader> blockHeaders) {
    return blockHeaders.stream().map(this::calculateBlobGasUsedRatio).toList();
  }

  private double calculateBlobGasUsedRatio(final BlockHeader blockHeader) {
    ProtocolSpec spec = protocolSchedule.getByBlockHeader(blockHeader);
    long blobGasUsed = blockHeader.getBlobGasUsed().orElse(0L);
    double currentBlobGasLimit = spec.getGasLimitCalculator().currentBlobGasLimit();
    if (currentBlobGasLimit == 0) {
      return 0;
    }
    return blobGasUsed / currentBlobGasLimit;
  }

  private FeeHistory.FeeHistoryResult createFeeHistoryResult(
      final long oldestBlock,
      final List<Wei> explicitlyRequestedBaseFees,
      final List<Wei> requestedBlobBaseFees,
      final Wei nextBaseFee,
      final List<Double> gasUsedRatios,
      final List<Double> blobGasUsedRatio,
      final Optional<List<List<Wei>>> maybeRewards) {
    return FeeHistory.FeeHistoryResult.from(
        ImmutableFeeHistory.builder()
            .oldestBlock(oldestBlock)
            .baseFeePerGas(
                Stream.concat(explicitlyRequestedBaseFees.stream(), Stream.of(nextBaseFee))
                    .toList())
            .baseFeePerBlobGas(requestedBlobBaseFees)
            .gasUsedRatio(gasUsedRatios)
            .blobGasUsedRatio(blobGasUsedRatio)
            .reward(maybeRewards)
            .build());
  }
}
