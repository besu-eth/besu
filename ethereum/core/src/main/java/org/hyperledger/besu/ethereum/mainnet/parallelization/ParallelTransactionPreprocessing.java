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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor.PreprocessingFunction;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

public class ParallelTransactionPreprocessing implements PreprocessingFunction {

  private final MainnetTransactionProcessor transactionProcessor;
  private final Executor executor;
  private final BalConfiguration balConfiguration;

  public ParallelTransactionPreprocessing(
      final MainnetTransactionProcessor transactionProcessor,
      final Executor executor,
      final BalConfiguration balConfiguration) {
    this.transactionProcessor = transactionProcessor;
    this.executor = executor;
    this.balConfiguration = balConfiguration;
  }

  @Override
  public Optional<PreprocessingContext> run(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final List<Transaction> transactions,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Wei blobGasPrice,
      final Optional<BlockAccessListBuilder> blockAccessListBuilder,
      final Optional<BlockAccessList> maybeBlockBal,
      final Optional<BlockHeader> maybeParentHeader) {
    if (!(protocolContext.getWorldStateArchive() instanceof PathBasedWorldStateProvider)) {
      return Optional.empty();
    }

    final ParallelBlockTransactionProcessor parallelProcessor;

    if (balConfiguration.isPerfectParallelizationEnabled() && maybeBlockBal.isPresent()) {
      parallelProcessor =
          new BalConcurrentTransactionProcessor(
              transactionProcessor, maybeBlockBal.get(), balConfiguration);
    } else {
      parallelProcessor = new ParallelizedConcurrentTransactionProcessor(transactionProcessor);
    }

    parallelProcessor.runAsyncBlock(
        protocolContext,
        blockHeader,
        transactions,
        miningBeneficiary,
        blockHashLookup,
        blobGasPrice,
        executor,
        blockAccessListBuilder,
        maybeParentHeader);

    return Optional.of(new PreprocessingContext(parallelProcessor));
  }
}
