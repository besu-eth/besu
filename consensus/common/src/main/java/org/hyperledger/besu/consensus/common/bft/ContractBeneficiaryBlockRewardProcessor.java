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
package org.hyperledger.besu.consensus.common.bft;

import static org.hyperledger.besu.evm.frame.MessageFrame.DEFAULT_MAX_STACK_SIZE;

import org.hyperledger.besu.consensus.common.bft.BeneficiaryContractDecoder.WeightedBeneficiary;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.mainnet.BlockRewardProcessor;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.processor.AbstractMessageProcessor;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Splits the block reward between the beneficiaries returned by a contract's {@code
 * getBeneficiaries()} view function, which returns {@code (address[] beneficiaries, uint256[]
 * weightsBps)}.
 *
 * <p>The contract is called read-only, in a static frame on a child of the world state being
 * processed that is never committed, and the shares are credited as direct balance increments, as a
 * single beneficiary's reward is. The call therefore sees the world state the block is executed on,
 * after the block's transactions, so block import and block creation compute the same split.
 *
 * <p>The call never fails a block. If the contract has no code, or the call reverts or runs out of
 * gas, or its output is not valid (see {@link BeneficiaryContractDecoder}), the whole reward is
 * paid exactly as it would be without a contract, to the mining beneficiary.
 *
 * <p>Each share is {@code reward * weightBps / 10000}, rounded down. The rounding remainder goes to
 * the first beneficiary. A share of zero is not credited, so it creates no empty account.
 */
public class ContractBeneficiaryBlockRewardProcessor implements BlockRewardProcessor {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContractBeneficiaryBlockRewardProcessor.class);

  /**
   * The gas available to the {@code getBeneficiaries()} call. A fixed protocol constant, so that no
   * node-local setting can make the call succeed on one node and fail on another.
   */
  public static final long CALL_GAS_LIMIT = 1_000_000L;

  /** The address the call is made from. */
  static final Address SYSTEM_ADDRESS =
      Address.fromHexString("0xfffffffffffffffffffffffffffffffffffffffe");

  /** The function selector of {@code getBeneficiaries()}. */
  static final Bytes GET_BENEFICIARIES_SELECTOR =
      Hash.hash(Bytes.wrap("getBeneficiaries()".getBytes(StandardCharsets.UTF_8)))
          .getBytes()
          .slice(0, 4);

  private final MainnetTransactionProcessor transactionProcessor;
  private final Address contractAddress;
  private final BlockRewardProcessor fallback;

  /**
   * Creates the processor.
   *
   * @param transactionProcessor the processor whose message-call processor runs the call
   * @param contractAddress the address of the contract to read
   * @param fallback the processor that pays the whole reward to the mining beneficiary, used
   *     whenever the contract cannot be used
   */
  public ContractBeneficiaryBlockRewardProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final Address contractAddress,
      final BlockRewardProcessor fallback) {
    this.transactionProcessor = transactionProcessor;
    this.contractAddress = contractAddress;
    this.fallback = fallback;
  }

  @Override
  public boolean rewardBeneficiaries(
      final MutableWorldState worldState,
      final ProcessableBlockHeader header,
      final List<BlockHeader> ommers,
      final Address miningBeneficiary) {
    // BFT blocks have no ommers; if there are any, the fallback rejects or pays them as usual.
    if (!ommers.isEmpty()) {
      return fallback.rewardBeneficiaries(worldState, header, ommers, miningBeneficiary);
    }

    final Optional<List<WeightedBeneficiary>> beneficiaries = readBeneficiaries(worldState, header);
    if (beneficiaries.isEmpty()) {
      return fallback.rewardBeneficiaries(worldState, header, ommers, miningBeneficiary);
    }

    final BigInteger reward = fallback.getBlockReward().toBigInteger();
    final List<WeightedBeneficiary> split = beneficiaries.get();
    final BigInteger[] shares = new BigInteger[split.size()];
    BigInteger distributed = BigInteger.ZERO;
    for (int i = 0; i < shares.length; i++) {
      shares[i] =
          reward
              .multiply(BigInteger.valueOf(split.get(i).weightBps()))
              .divide(BigInteger.valueOf(BeneficiaryContractDecoder.TOTAL_WEIGHT_BPS));
      distributed = distributed.add(shares[i]);
    }
    shares[0] = shares[0].add(reward.subtract(distributed));

    final WorldUpdater updater = worldState.updater();
    for (int i = 0; i < shares.length; i++) {
      if (shares[i].signum() > 0) {
        updater.getOrCreate(split.get(i).address()).incrementBalance(Wei.of(shares[i]));
      }
    }
    updater.commit();
    return true;
  }

  /**
   * Calls {@code getBeneficiaries()}. The call is made on a child of the world state's updater that
   * is dropped without being committed, so it cannot change any state.
   */
  private Optional<List<WeightedBeneficiary>> readBeneficiaries(
      final MutableWorldState worldState, final ProcessableBlockHeader header) {
    try {
      final WorldUpdater callUpdater = worldState.updater().updater();
      final Account contract = callUpdater.get(contractAddress);
      if (contract == null || contract.getCode().isEmpty()) {
        LOG.warn(
            "Block {}: no code at beneficiary contract {}, paying the block reward to the mining beneficiary",
            header.getNumber(),
            contractAddress);
        return Optional.empty();
      }

      final AbstractMessageProcessor processor =
          transactionProcessor.getMessageProcessor(MessageFrame.Type.MESSAGE_CALL);
      final MessageFrame frame =
          MessageFrame.builder()
              .maxStackSize(DEFAULT_MAX_STACK_SIZE)
              .worldUpdater(callUpdater.updater())
              .initialGas(CALL_GAS_LIMIT)
              .originator(SYSTEM_ADDRESS)
              .sender(SYSTEM_ADDRESS)
              .gasPrice(Wei.ZERO)
              .blobGasPrice(Wei.ZERO)
              .value(Wei.ZERO)
              .apparentValue(Wei.ZERO)
              .blockValues(header)
              .completer(__ -> {})
              .miningBeneficiary(Address.ZERO)
              .type(MessageFrame.Type.MESSAGE_CALL)
              .address(contractAddress)
              .contract(contractAddress)
              .inputData(GET_BENEFICIARIES_SELECTOR)
              .isStatic(true)
              // BLOCKHASH returns zero: the call must not depend on node-local chain access.
              .blockHashLookup((BlockHashLookup) (messageFrame, blockNumber) -> Hash.ZERO)
              .enableEvmV2(processor.runsOnEvmV2())
              .code(getCode(contract, processor))
              .build();

      final Deque<MessageFrame> stack = frame.getMessageFrameStack();
      while (!stack.isEmpty()) {
        final MessageFrame current = stack.peekFirst();
        transactionProcessor
            .getMessageProcessor(current.getType())
            .process(current, OperationTracer.NO_TRACING);
      }

      if (frame.getState() != MessageFrame.State.COMPLETED_SUCCESS) {
        LOG.warn(
            "Block {}: beneficiary contract call did not succeed ({}), paying the block reward to the mining beneficiary",
            header.getNumber(),
            frame.getExceptionalHaltReason().map(Object::toString).orElse(frame.getState().name()));
        return Optional.empty();
      }

      final Optional<List<WeightedBeneficiary>> beneficiaries =
          BeneficiaryContractDecoder.decode(frame.getOutputData());
      if (beneficiaries.isEmpty()) {
        LOG.warn(
            "Block {}: beneficiary contract returned invalid data, paying the block reward to the mining beneficiary",
            header.getNumber());
      }
      return beneficiaries;
    } catch (final RuntimeException e) {
      LOG.warn(
          "Block {}: beneficiary contract call failed, paying the block reward to the mining beneficiary",
          header.getNumber(),
          e);
      return Optional.empty();
    }
  }

  private static Code getCode(final Account contract, final AbstractMessageProcessor processor) {
    // Bonsai accounts may have a fully cached code, so we use that one
    if (contract.getCodeCache() != null) {
      return contract.getOrCreateCachedCode();
    }
    return processor.getOrCreateCachedJumpDest(contract.getCodeHash(), contract.getCode());
  }

  @Override
  public Wei getBlockReward() {
    return fallback.getBlockReward();
  }

  @Override
  public Wei getCoinbaseReward(final int numberOfOmmers) {
    return fallback.getCoinbaseReward(numberOfOmmers);
  }

  @Override
  public Wei getOmmerReward(final long blockNumber, final long ommerBlockNumber) {
    return fallback.getOmmerReward(blockNumber, ommerBlockNumber);
  }
}
