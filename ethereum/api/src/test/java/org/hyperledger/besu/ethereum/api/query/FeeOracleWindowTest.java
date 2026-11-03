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
package org.hyperledger.besu.ethereum.api.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.query.FeeOracleWindow.Percentiles;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FeeOracleWindowTest {

  private static final Wei TWO_TO_THE_63 = Wei.of(BigInteger.TWO.pow(63));
  private static final Wei TWO_TO_THE_64 = Wei.of(BigInteger.TWO.pow(64));

  private final Blockchain blockchain = mock(Blockchain.class);
  private final Map<Hash, Block> blocksByHash = new HashMap<>();
  private final Set<Hash> missingBodies = new HashSet<>();
  private Hash chainHeadHash = Hash.ZERO;

  @BeforeEach
  void setUp() {
    when(blockchain.getChainHeadHash()).thenAnswer(invocation -> chainHeadHash);
    when(blockchain.getBlockHeader(any(Hash.class)))
        .thenAnswer(
            invocation ->
                Optional.ofNullable(blocksByHash.get(invocation.getArgument(0, Hash.class)))
                    .map(Block::getHeader));
    when(blockchain.getBlockBody(any(Hash.class)))
        .thenAnswer(
            invocation -> {
              final Hash hash = invocation.getArgument(0, Hash.class);
              return missingBodies.contains(hash)
                  ? Optional.empty()
                  : Optional.ofNullable(blocksByHash.get(hash)).map(Block::getBody);
            });
  }

  @Test
  void newHeadReadsOnlyTheNewBlock() {
    final List<Block> chain = chain(200);
    final FeeOracleWindow window = new FeeOracleWindow(blockchain, 100, 0.5);
    window.percentilesAt(chain.get(199).getHeader());
    verify(blockchain, times(100)).getBlockBody(any(Hash.class));
    verify(blockchain, times(99)).getBlockHeader(any(Hash.class));
    clearInvocations(blockchain);

    final Optional<Percentiles> percentiles = window.percentilesAt(chain.get(200).getHeader());

    verify(blockchain).getBlockBody(chain.get(200).getHash());
    verify(blockchain, times(1)).getBlockBody(any(Hash.class));
    verify(blockchain, never()).getBlockHeader(any(Hash.class));
    // Blocks 101-200: gas prices 1010-2000 and priority fees 101-200.
    assertThat(percentiles)
        .contains(new Percentiles(Optional.of(Wei.of(1510)), Optional.of(Wei.of(151))));
  }

  @Test
  void blocksOfANewBranchAreReadAndTheOldBranchIsNotUsed() {
    final List<Block> chain = chain(200);
    final FeeOracleWindow window = new FeeOracleWindow(blockchain, 100, 0.5);
    final Optional<Percentiles> oldBranch = window.percentilesAt(chain.get(200).getHeader());
    final Block fork199 = block(chain.get(198).getHash(), 199, Wei.of(1), Wei.of(1), 1);
    final Block fork200 = block(fork199.getHash(), 200, Wei.of(2), Wei.of(2), 1);
    clearInvocations(blockchain);

    final Optional<Percentiles> newBranch = window.percentilesAt(fork200.getHeader());

    verify(blockchain).getBlockBody(fork200.getHash());
    verify(blockchain).getBlockBody(fork199.getHash());
    verify(blockchain, times(2)).getBlockBody(any(Hash.class));
    verify(blockchain).getBlockHeader(fork199.getHash());
    verify(blockchain, times(1)).getBlockHeader(any(Hash.class));
    // Blocks 101-198 and the two new ones: gas prices 1, 2 and 1010-1980.
    assertThat(newBranch)
        .contains(new Percentiles(Optional.of(Wei.of(1490)), Optional.of(Wei.of(149))))
        .isNotEqualTo(oldBranch);
  }

  @Test
  void keepsTheFeesOfAtMost1024Blocks() {
    final List<Block> chain = chain(1101);
    final FeeOracleWindow window = new FeeOracleWindow(blockchain, 1100, 0.5);
    window.percentilesAt(chain.get(1100).getHeader());
    clearInvocations(blockchain);

    final Optional<Percentiles> percentiles = window.percentilesAt(chain.get(1101).getHeader());

    // The window is blocks 2-1101 and blocks 77-1100 were kept, so 1101 and 2-76 are read.
    verify(blockchain).getBlockBody(chain.get(1101).getHash());
    verify(blockchain).getBlockBody(chain.get(2).getHash());
    verify(blockchain).getBlockBody(chain.get(76).getHash());
    verify(blockchain, never()).getBlockBody(chain.get(77).getHash());
    verify(blockchain, times(76)).getBlockBody(any(Hash.class));
    verify(blockchain, times(75)).getBlockHeader(any(Hash.class));
    assertThat(percentiles).isEqualTo(freshWindowAt(chain.get(1101), 1100));
  }

  @Test
  void missingBodyGivesNoPercentilesAndKeepsTheWindow() {
    final List<Block> chain = chain(201);
    final FeeOracleWindow window = new FeeOracleWindow(blockchain, 100, 0.5);
    window.percentilesAt(chain.get(200).getHeader());
    missingBodies.add(chain.get(201).getHash());

    assertThat(window.percentilesAt(chain.get(201).getHeader())).isEmpty();

    missingBodies.clear();
    clearInvocations(blockchain);
    final Optional<Percentiles> percentiles = window.percentilesAt(chain.get(201).getHeader());
    verify(blockchain, times(1)).getBlockBody(any(Hash.class));
    assertThat(percentiles).isEqualTo(freshWindowAt(chain.get(201), 100));
  }

  @Test
  void missingOlderBodyGivesNoPercentiles() {
    final List<Block> chain = chain(200);
    missingBodies.add(chain.get(150).getHash());

    assertThat(new FeeOracleWindow(blockchain, 100, 0.5).percentilesAt(chain.get(200).getHeader()))
        .isEmpty();
  }

  @Test
  void olderHeadDoesNotReplaceTheWindowOfANewerHead() {
    final List<Block> chain = chain(201);
    final FeeOracleWindow window = new FeeOracleWindow(blockchain, 100, 0.5);
    chainHeadHash = chain.get(200).getHash();
    window.percentilesAt(chain.get(200).getHeader());
    // A computation for head 150 that finishes after the chain reached head 200.
    window.percentilesAt(chain.get(150).getHeader());
    chainHeadHash = chain.get(201).getHash();
    clearInvocations(blockchain);

    window.percentilesAt(chain.get(201).getHeader());

    verify(blockchain, times(1)).getBlockBody(any(Hash.class));
  }

  @Test
  void lowerChainHeadAfterAReorgReplacesTheWindow() {
    final List<Block> chain = chain(200);
    final FeeOracleWindow window = new FeeOracleWindow(blockchain, 100, 0.5);
    chainHeadHash = chain.get(200).getHash();
    window.percentilesAt(chain.get(200).getHeader());
    final List<Block> branch = new ArrayList<>();
    Hash parentHash = chain.get(150).getHash();
    for (int number = 151; number <= 161; number++) {
      final Block block = block(parentHash, number, Wei.of(5L * number), Wei.of(2L * number), 1);
      branch.add(block);
      parentHash = block.getHash();
    }
    final Block branchHead = branch.get(9);
    final Block nextBranchHead = branch.get(10);
    chainHeadHash = branchHead.getHash();
    window.percentilesAt(branchHead.getHeader());
    chainHeadHash = nextBranchHead.getHash();
    clearInvocations(blockchain);

    final Optional<Percentiles> percentiles = window.percentilesAt(nextBranchHead.getHeader());

    verify(blockchain).getBlockBody(nextBranchHead.getHash());
    verify(blockchain, times(1)).getBlockBody(any(Hash.class));
    assertThat(percentiles).isEqualTo(freshWindowAt(nextBranchHead, 100));
  }

  @Test
  void windowStartsAtGenesisOnAShortChain() {
    final List<Block> chain = chain(10);

    final Optional<Percentiles> percentiles =
        new FeeOracleWindow(blockchain, 100, 0.5).percentilesAt(chain.get(10).getHeader());

    verify(blockchain, times(11)).getBlockBody(any(Hash.class));
    // Blocks 0-10: gas prices 0-100 and priority fees 0-10.
    assertThat(percentiles)
        .contains(new Percentiles(Optional.of(Wei.of(50)), Optional.of(Wei.of(5))));
  }

  @Test
  void blocksWithoutTransactionsGiveNoPercentiles() {
    final Block genesis = add(header(Hash.ZERO, 0, 0), List.of());
    final Block head = add(header(genesis.getHash(), 1, 0), List.of());

    assertThat(new FeeOracleWindow(blockchain, 100, 0.5).percentilesAt(head.getHeader()))
        .contains(new Percentiles(Optional.empty(), Optional.empty()));
  }

  @ParameterizedTest
  @CsvSource({
    "0.0, 1",
    "0.5, 9223372036854775807",
    "0.7, 9223372036854775808",
    "0.9, 18446744073709551616"
  })
  void feesFromTwoToThe63AreCounted(final double percentile, final BigInteger expected) {
    final Block head =
        add(
            header(Hash.ZERO, 0, 0),
            List.of(
                legacy(Wei.of(5)),
                legacy(TWO_TO_THE_63),
                legacy(Wei.of(1)),
                legacy(TWO_TO_THE_64),
                legacy(Wei.of(3)),
                legacy(Wei.of(Long.MAX_VALUE))));

    assertThat(new FeeOracleWindow(blockchain, 1, percentile).percentilesAt(head.getHeader()))
        .contains(new Percentiles(Optional.of(Wei.of(expected)), Optional.empty()));
  }

  private Optional<Percentiles> freshWindowAt(final Block head, final long windowBlocks) {
    return new FeeOracleWindow(blockchain, windowBlocks, 0.5).percentilesAt(head.getHeader());
  }

  // Block n has a legacy transaction with a gas price of 10n and an EIP-1559 transaction with a
  // priority fee of n.
  private List<Block> chain(final int headNumber) {
    final List<Block> chain = new ArrayList<>();
    Hash parentHash = Hash.ZERO;
    for (int number = 0; number <= headNumber; number++) {
      final Block block = block(parentHash, number, Wei.of(10L * number), Wei.of(number), 0);
      chain.add(block);
      parentHash = block.getHash();
    }
    return chain;
  }

  private Block block(
      final Hash parentHash,
      final long number,
      final Wei gasPrice,
      final Wei priorityFee,
      final int branch) {
    return add(header(parentHash, number, branch), List.of(legacy(gasPrice), eip1559(priorityFee)));
  }

  private static BlockHeader header(final Hash parentHash, final long number, final int branch) {
    return new BlockHeaderTestFixture()
        .parentHash(parentHash)
        .number(number)
        .extraData(Bytes.of(branch))
        .buildHeader();
  }

  private Block add(final BlockHeader header, final List<Transaction> transactions) {
    final Block block = new Block(header, new BlockBody(transactions, List.of()));
    blocksByHash.put(block.getHash(), block);
    return block;
  }

  private static Transaction legacy(final Wei gasPrice) {
    return new Transaction.Builder()
        .type(TransactionType.FRONTIER)
        .nonce(0)
        .gasPrice(gasPrice)
        .gasLimit(21_000)
        .value(Wei.ZERO)
        .build();
  }

  private static Transaction eip1559(final Wei priorityFee) {
    return new Transaction.Builder()
        .chainId(BigInteger.ONE)
        .type(TransactionType.EIP1559)
        .nonce(0)
        .maxFeePerGas(priorityFee.add(Wei.of(1_000)))
        .maxPriorityFeePerGas(priorityFee)
        .gasLimit(21_000)
        .value(Wei.ZERO)
        .build();
  }
}
