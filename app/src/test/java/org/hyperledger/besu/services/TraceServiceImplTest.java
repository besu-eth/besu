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
package org.hyperledger.besu.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.worldstate.WorldView;
import org.hyperledger.besu.plugin.data.BlockBody;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.data.BlockTraceResult;
import org.hyperledger.besu.plugin.data.TransactionTraceResult;
import org.hyperledger.besu.plugin.services.TraceService;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.tracer.BlockAwareOperationTracer;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.LongStream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TraceServiceImplTest {

  TraceService traceService;
  private MutableBlockchain blockchain;
  private WorldStateArchive worldStateArchive;

  /**
   * The blockchain for testing has a height of 32 and the account
   * 0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b makes a transaction per block. So, in the head, the
   * nonce of this account should be 32.
   */
  @BeforeEach
  public void setup() {
    final BlockchainSetupUtil blockchainSetupUtil =
        BlockchainSetupUtil.forTesting(DataStorageFormat.BONSAI);
    blockchainSetupUtil.importAllBlocks();
    blockchain = blockchainSetupUtil.getBlockchain();
    worldStateArchive = blockchainSetupUtil.getWorldArchive();
    BlockchainQueries blockchainQueries =
        new BlockchainQueries(
            blockchainSetupUtil.getProtocolSchedule(),
            blockchain,
            worldStateArchive,
            MiningConfiguration.newDefault());
    traceService =
        new TraceServiceImpl(blockchainQueries, blockchainSetupUtil.getProtocolSchedule());
  }

  @Test
  void shouldRetrieveStateUpdatePostTracingForOneBlock() {

    final Address addressToVerify =
        Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b");

    final long persistedNonceForAccount =
        worldStateArchive.getWorldState().get(addressToVerify).getNonce();

    final long blockNumber = 2;

    final BlockAwareOperationTracer opTracer = spy(new BlockAwareOperationTracer() {});

    traceService.trace(
        blockNumber,
        blockNumber,
        worldState -> assertThat(worldState.get(addressToVerify).getNonce()).isEqualTo(1),
        worldState -> assertThat(worldState.get(addressToVerify).getNonce()).isEqualTo(2),
        opTracer);

    assertThat(worldStateArchive.getWorldState().get(addressToVerify).getNonce())
        .isEqualTo(persistedNonceForAccount);

    final Block tracedBlock = blockchain.getBlockByNumber(blockNumber).orElseThrow();

    verify(opTracer)
        .traceStartBlock(
            any(),
            eq(tracedBlock.getHeader()),
            eq(tracedBlock.getBody()),
            eq(tracedBlock.getHeader().getCoinbase()));

    tracedBlock
        .getBody()
        .getTransactions()
        .forEach(
            tx -> {
              verify(opTracer).tracePrepareTransaction(any(), eq(tx));
              verify(opTracer).traceStartTransaction(any(), eq(tx));
              verify(opTracer)
                  .traceEndTransaction(
                      any(), eq(tx), anyBoolean(), any(), any(), anyLong(), any(), anyLong());
            });

    verify(opTracer).traceEndBlock(tracedBlock.getHeader(), tracedBlock.getBody());
  }

  @Test
  void shouldRetrieveStateUpdatePostTracingForAllBlocks() {
    final Address addressToVerify =
        Address.fromHexString("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b");

    final long persistedNonceForAccount =
        worldStateArchive.getWorldState().get(addressToVerify).getNonce();

    final long startBlock = 1;
    final long endBlock = 32;
    final BlockAwareOperationTracer opTracer = spy(new BlockAwareOperationTracer() {});

    traceService.trace(
        startBlock,
        endBlock,
        worldState -> assertThat(worldState.get(addressToVerify).getNonce()).isEqualTo(0),
        worldState ->
            assertThat(worldState.get(addressToVerify).getNonce())
                .isEqualTo(persistedNonceForAccount),
        opTracer);

    assertThat(worldStateArchive.getWorldState().get(addressToVerify).getNonce())
        .isEqualTo(persistedNonceForAccount);

    LongStream.rangeClosed(startBlock, endBlock)
        .mapToObj(blockchain::getBlockByNumber)
        .map(Optional::orElseThrow)
        .forEach(
            tracedBlock -> {
              verify(opTracer)
                  .traceStartBlock(
                      any(),
                      eq(tracedBlock.getHeader()),
                      eq(tracedBlock.getBody()),
                      eq(tracedBlock.getHeader().getCoinbase()));
              tracedBlock
                  .getBody()
                  .getTransactions()
                  .forEach(
                      tx -> {
                        verify(opTracer).tracePrepareTransaction(any(), eq(tx));
                        verify(opTracer).traceStartTransaction(any(), eq(tx));
                        verify(opTracer)
                            .traceEndTransaction(
                                any(),
                                eq(tx),
                                anyBoolean(),
                                any(),
                                any(),
                                anyLong(),
                                any(),
                                anyLong());
                      });

              verify(opTracer).traceEndBlock(tracedBlock.getHeader(), tracedBlock.getBody());
            });
  }

  @Test
  void shouldReturnTheCorrectWorldViewForTxStartEnd() {
    final TxStartEndTracer txStartEndTracer = new TxStartEndTracer();

    // block contains 1 transaction
    final BlockTraceResult blockTraceResult = traceService.traceBlock(31, txStartEndTracer);

    assertThat(blockTraceResult).isNotNull();

    final List<TransactionTraceResult> transactionTraceResults =
        blockTraceResult.transactionTraceResults();
    assertThat(transactionTraceResults.size()).isEqualTo(1);

    assertThat(transactionTraceResults.getFirst().getTxHash()).isNotNull();
    assertThat(transactionTraceResults.getFirst().getStatus())
        .isEqualTo(TransactionTraceResult.Status.SUCCESS);
    assertThat(transactionTraceResults.getFirst().errorMessage()).isEmpty();

    assertThat(txStartEndTracer.txStartWorldView).isNotNull();
    assertThat(txStartEndTracer.txEndWorldView).isNotNull();

    assertThat(txStartEndTracer.txStartTransaction.getNonce())
        .isEqualTo(txStartEndTracer.txEndTransaction.getNonce())
        .isEqualTo(30);
    assertThat(txStartEndTracer.txStartTransaction.getGasLimit())
        .isEqualTo(txStartEndTracer.txEndTransaction.getGasLimit())
        .isEqualTo(314159);
    assertThat(txStartEndTracer.txStartTransaction.getTo().orElseThrow())
        .isEqualTo(txStartEndTracer.txEndTransaction.getTo().orElseThrow())
        .isEqualTo(Address.fromHexString("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"));
    assertThat(txStartEndTracer.txStartTransaction.getValue())
        .isEqualTo(txStartEndTracer.txEndTransaction.getValue())
        .isEqualTo(
            Wei.fromHexString(
                "0x000000000000000000000000000000000000000000000000000000000000000a"));
    assertThat(txStartEndTracer.txStartTransaction.getPayload())
        .isEqualTo(txStartEndTracer.txEndTransaction.getPayload())
        .isEqualTo(Bytes.fromHexString("0xfd408767"));

    assertThat(txStartEndTracer.txEndStatus).isTrue();
    assertThat(txStartEndTracer.txEndOutput).isEqualTo(Bytes.fromHexString("0x"));
    assertThat(txStartEndTracer.txEndGasUsed).isEqualTo(24303);
    assertThat(txStartEndTracer.txEndSelfDestructs).isEmpty();
    assertThat(txStartEndTracer.txEndTimeNs).isNotNull();

    assertThat(txStartEndTracer.txEndLogs).isNotEmpty();

    final Log actualLog = txStartEndTracer.txEndLogs.getFirst();
    assertThat(actualLog.getLogger())
        .isEqualTo(Address.fromHexString("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"));
    assertThat(actualLog.getData())
        .isEqualTo(
            Bytes.fromHexString(
                "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe9000000000000000000000000000000000000000000000000000000000000002a"));
    assertThat(actualLog.getTopics()).hasSize(4);
    assertThat(actualLog.getTopics().getFirst().getBytes())
        .isEqualTo(
            Bytes.fromHexString(
                "0xd5f0a30e4be0c6be577a71eceb7464245a796a7e6a55c0d971837b250de05f4e"));
    assertThat(actualLog.getTopics().get(1).getBytes())
        .isEqualTo(
            Bytes.fromHexString(
                "0x0000000000000000000000000000000000000000000000000000000000000001"));
    assertThat(actualLog.getTopics().get(2).getBytes())
        .isEqualTo(
            Bytes.fromHexString(
                "0x000000000000000000000000a94f5374fce5edbc8e2a8697c15331677e6ebf0b"));
    assertThat(actualLog.getTopics().get(3).getBytes())
        .isEqualTo(
            Bytes.fromHexString(
                "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"));
  }

  private static class TxStartEndTracer implements BlockAwareOperationTracer {
    public WorldView txStartWorldView;
    public WorldView txEndWorldView;

    public Transaction txStartTransaction;
    public Transaction txEndTransaction;

    public boolean txEndStatus;
    public Bytes txEndOutput;
    public List<Log> txEndLogs;
    public long txEndGasUsed;
    public Set<Address> txEndSelfDestructs;
    public Long txEndTimeNs;

    private final Set<Transaction> traceStartTxCalled = new HashSet<>();
    private final Set<Transaction> traceEndTxCalled = new HashSet<>();
    private final Set<Hash> traceStartBlockCalled = new HashSet<>();
    private final Set<Hash> traceEndBlockCalled = new HashSet<>();

    @Override
    public void traceStartTransaction(final WorldView worldView, final Transaction transaction) {
      if (!traceStartTxCalled.add(transaction)) {
        fail("traceStartTransaction already called for tx " + transaction);
      }
      txStartWorldView = worldView;
      txStartTransaction = transaction;
    }

    @Override
    public void traceEndTransaction(
        final WorldView worldView,
        final Transaction transaction,
        final boolean status,
        final Bytes output,
        final List<Log> logs,
        final long gasUsed,
        final Set<Address> selfDestructs,
        final long timeNs) {
      if (!traceEndTxCalled.add(transaction)) {
        fail("traceEndTransaction already called for tx " + transaction);
      }
      txEndWorldView = worldView;
      txEndTransaction = transaction;
      txEndStatus = status;
      txEndOutput = output;
      txEndLogs = logs;
      txEndGasUsed = gasUsed;
      txEndSelfDestructs = selfDestructs;
      txEndTimeNs = timeNs;
    }

    @Override
    public void traceStartBlock(
        final WorldView worldView,
        final BlockHeader blockHeader,
        final BlockBody blockBody,
        final Address miningBeneficiary) {
      if (!traceStartBlockCalled.add(blockHeader.getBlockHash())) {
        fail("traceStartBlock already called for block " + blockHeader);
      }
    }

    @Override
    public void traceEndBlock(final BlockHeader blockHeader, final BlockBody blockBody) {
      if (!traceEndBlockCalled.add(blockHeader.getBlockHash())) {
        fail("traceEndBlock already called for block " + blockHeader);
      }
    }
  }
}
