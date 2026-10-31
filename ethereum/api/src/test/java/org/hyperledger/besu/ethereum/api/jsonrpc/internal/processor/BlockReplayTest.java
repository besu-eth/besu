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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

public class BlockReplayTest {

  private static final Address TOUCHED = Address.fromHexString("0x1234");

  private final BlockDataGenerator gen = new BlockDataGenerator();
  private final BlockHeader header = gen.header();
  private final Transaction earlierTransaction = gen.transaction();
  private final Transaction targetTransaction = gen.transaction();

  private final Blockchain blockchain = mock(Blockchain.class);
  private final ProtocolSchedule protocolSchedule = mock(ProtocolSchedule.class);
  private final ProtocolSpec protocolSpec = mock(ProtocolSpec.class, Answers.RETURNS_DEEP_STUBS);
  private final MainnetTransactionProcessor transactionProcessor =
      mock(MainnetTransactionProcessor.class);
  private final BlockchainQueries blockchainQueries = mock(BlockchainQueries.class);

  private BlockReplay blockReplay;

  @BeforeEach
  public void setUp() {
    when(blockchain.getBlockHeader(header.getHash())).thenReturn(Optional.of(header));
    when(blockchain.getBlockBody(header.getHash()))
        .thenReturn(
            Optional.of(new BlockBody(List.of(earlierTransaction, targetTransaction), List.of())));
    when(protocolSchedule.getByBlockHeader(header)).thenReturn(protocolSpec);
    when(protocolSpec.getTransactionProcessor()).thenReturn(transactionProcessor);

    // The earlier transaction credits an account; the target transaction must see that credit.
    // Like MainnetTransactionProcessor, write through a child updater and commit only the child
    // into the updater that was passed in.
    when(transactionProcessor.processTransaction(
            any(WorldUpdater.class),
            any(ProcessableBlockHeader.class),
            eq(earlierTransaction),
            nullable(Address.class),
            nullable(BlockHashLookup.class),
            any(TransactionValidationParams.class),
            nullable(Wei.class)))
        .thenAnswer(
            invocation -> {
              final WorldUpdater updater = invocation.getArgument(0);
              final WorldUpdater transactionUpdater = updater.updater();
              transactionUpdater.getOrCreate(TOUCHED).setBalance(Wei.of(42));
              transactionUpdater.commit();
              return null;
            });

    blockReplay = new BlockReplay(protocolSchedule, mock(ProtocolContext.class), blockchain);
  }

  @Test
  public void forestReplayAppliesEarlierTransactionsBeforeTarget() {
    assertTargetSeesEarlierTransaction(InMemoryKeyValueStorageProvider.createInMemoryWorldState());
  }

  @Test
  public void bonsaiReplayAppliesEarlierTransactionsBeforeTarget() {
    assertTargetSeesEarlierTransaction(
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain)
            .getWorldState());
  }

  private void assertTargetSeesEarlierTransaction(final MutableWorldState parentState) {
    when(blockchainQueries.getBlockHeaderByHash(header.getHash())).thenReturn(Optional.of(header));
    when(blockchainQueries.getAndMapWorldState(eq(header.getParentHash()), any()))
        .thenAnswer(
            invocation -> {
              final Function<MutableWorldState, Optional<?>> mapper = invocation.getArgument(1);
              return mapper.apply(parentState);
            });

    final Optional<Wei> balanceSeenByTarget =
        Tracer.processTracing(
            blockchainQueries,
            header.getHash(),
            traceableState ->
                blockReplay.beforeTransactionInBlock(
                    traceableState,
                    header.getHash(),
                    targetTransaction.getHash(),
                    (transaction, index, block, chain, processor, blobGasPrice) ->
                        Optional.ofNullable(traceableState.updater().get(TOUCHED))
                            .map(Account::getBalance)
                            .orElse(Wei.ZERO)));

    assertThat(balanceSeenByTarget).contains(Wei.of(42));
  }
}
