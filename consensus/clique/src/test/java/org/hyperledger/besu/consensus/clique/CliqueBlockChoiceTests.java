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
package org.hyperledger.besu.consensus.clique;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.consensus.clique.CliqueHelpers.distanceFromInTurn;
import static org.hyperledger.besu.consensus.clique.CliqueHelpers.installCliqueBlockChoiceRule;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryBlockchain;
import static org.hyperledger.besu.ethereum.core.Util.publicKeyToAddress;

import org.hyperledger.besu.consensus.common.BlockInterface;
import org.hyperledger.besu.consensus.common.EpochManager;
import org.hyperledger.besu.consensus.common.validator.blockbased.BlockValidatorProvider;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.Difficulty;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CliqueBlockChoiceTests {
  private List<KeyPair> keyPairs;
  private List<Address> addresses;
  private BlockHeaderTestFixture headerBuilder;
  private MutableBlockchain blockchain;
  private CliqueContext cliqueContext;

  private Block createEmptyBlock(final KeyPair blockSigner) {
    final BlockHeader header =
        TestHelpers.createCliqueSignedBlockHeader(headerBuilder, blockSigner, addresses);
    return new Block(header, new BlockBody(Lists.newArrayList(), Lists.newArrayList()));
  }

  @BeforeEach
  public void setup() {
    keyPairs =
        IntStream.range(0, 8)
            .mapToObj(i -> SignatureAlgorithmFactory.getInstance().generateKeyPair())
            .sorted(Comparator.comparing(kp -> publicKeyToAddress(kp.getPublicKey())))
            .collect(Collectors.toList());
    addresses =
        keyPairs.stream()
            .map(kp -> publicKeyToAddress(kp.getPublicKey()))
            .collect(Collectors.toList());
    headerBuilder = new BlockHeaderTestFixture();
    final Block genesisBlock = createEmptyBlock(keyPairs.get(0));
    blockchain = createInMemoryBlockchain(genesisBlock, new CliqueBlockHeaderFunctions());
    final EpochManager epochManager = new EpochManager(30_000);
    final BlockInterface blockInterface = new CliqueBlockInterface();

    cliqueContext =
        new CliqueContext(
            BlockValidatorProvider.nonForkingValidatorProvider(
                blockchain, epochManager, blockInterface),
            epochManager,
            blockInterface);

    installCliqueBlockChoiceRule(blockchain, cliqueContext);
    for (int i = 1; i < keyPairs.size(); i++) {
      headerBuilder.number(i);
      headerBuilder.parentHash(blockchain.getChainHeadHash());
      blockchain.appendBlock(createEmptyBlock(keyPairs.get(i)), List.of());
    }
  }

  @Test
  public void highestDifficultyPreferred() {
    headerBuilder.number(8);

    headerBuilder.difficulty(Difficulty.of(2));
    final Block betterBlock = createEmptyBlock(keyPairs.get(0));
    final BlockHeader betterHeader = betterBlock.getHeader();

    headerBuilder.difficulty(Difficulty.of(1));
    final Block worseBlock = createEmptyBlock(keyPairs.get(1));
    final BlockHeader worseHeader = worseBlock.getHeader();

    // No prior fork choices to verify are equal

    final Comparator<BlockHeader> blockChoiceRule = blockchain.getBlockChoiceRule();
    assertThat(blockChoiceRule.compare(worseHeader, betterHeader)).isNegative();
    assertThat(blockChoiceRule.compare(betterHeader, worseHeader)).isPositive();
  }

  @Test
  public void shorterChainIsPreferred() {
    headerBuilder.number(8);

    headerBuilder.difficulty(Difficulty.of(2));
    final Block betterBlock = createEmptyBlock(keyPairs.get(0));
    final BlockHeader betterHeader = betterBlock.getHeader();

    headerBuilder.difficulty(Difficulty.of(1));
    final Block hiddenBlock = createEmptyBlock(keyPairs.get(1));
    blockchain.appendBlock(hiddenBlock, List.of());

    headerBuilder.number(9);
    headerBuilder.difficulty(Difficulty.of(1));
    headerBuilder.parentHash(hiddenBlock.getHash());
    final Block worseBlock = createEmptyBlock(keyPairs.get(2));
    final BlockHeader worseHeader = worseBlock.getHeader();

    final Comparator<BlockHeader> blockChoiceRule = blockchain.getBlockChoiceRule();

    // verify total difficulty is equal
    assertThat(blockchain.getTotalDifficultyByHash(worseHeader.getHash()))
        .isEqualTo(blockchain.getTotalDifficultyByHash(betterHeader.getHash()));

    assertThat(blockChoiceRule.compare(worseHeader, betterHeader)).isNegative();
    assertThat(blockChoiceRule.compare(betterHeader, worseHeader)).isPositive();
  }

  @Test
  public void leastRecentInTurnIsPreferred() {
    headerBuilder.number(8);

    headerBuilder.difficulty(Difficulty.of(1));
    final Block betterBlock = createEmptyBlock(keyPairs.get(3));
    final BlockHeader betterHeader = betterBlock.getHeader();

    headerBuilder.difficulty(Difficulty.of(1));
    final Block worseBlock = createEmptyBlock(keyPairs.get(2));
    final BlockHeader worseHeader = worseBlock.getHeader();

    // verify total difficulty is equal
    assertThat(blockchain.getTotalDifficultyByHash(worseHeader.getHash()))
        .isEqualTo(blockchain.getTotalDifficultyByHash(betterHeader.getHash()));
    // verify chains are same length
    assertThat(worseHeader.getNumber()).isEqualTo(betterHeader.getNumber());

    final Comparator<BlockHeader> blockChoiceRule = blockchain.getBlockChoiceRule();
    assertThat(blockChoiceRule.compare(worseHeader, betterHeader)).isNegative();
    assertThat(blockChoiceRule.compare(betterHeader, worseHeader)).isPositive();
  }

  @Test
  public void lowestHashIsPreferred() {
    headerBuilder.number(8);

    headerBuilder.difficulty(Difficulty.of(1));
    headerBuilder.timestamp(System.currentTimeMillis() / 1000);
    final Block firstBlock = createEmptyBlock(keyPairs.get(1));

    headerBuilder.difficulty(Difficulty.of(1));
    headerBuilder.timestamp(firstBlock.getHeader().getTimestamp() + 1);
    final Block secondBlock = createEmptyBlock(keyPairs.get(1));

    final BlockHeader betterHeader;
    final BlockHeader worseHeader;
    if (firstBlock.getHash().getBytes().compareTo(secondBlock.getHash().getBytes()) > 0) {
      worseHeader = firstBlock.getHeader();
      betterHeader = secondBlock.getHeader();
    } else {
      worseHeader = secondBlock.getHeader();
      betterHeader = firstBlock.getHeader();
    }

    // verify total difficulty is equal
    assertThat(blockchain.getTotalDifficultyByHash(worseHeader.getHash()))
        .isEqualTo(blockchain.getTotalDifficultyByHash(betterHeader.getHash()));
    // verify chains are the same length
    assertThat(worseHeader.getNumber()).isEqualTo(betterHeader.getNumber());
    // verify signer is the same distance to in-turn
    assertThat(distanceFromInTurn(worseHeader, cliqueContext))
        .isEqualTo(distanceFromInTurn(betterHeader, cliqueContext));

    final Comparator<BlockHeader> blockChoiceRule = blockchain.getBlockChoiceRule();
    assertThat(blockChoiceRule.compare(worseHeader, betterHeader)).isNegative();
    assertThat(blockChoiceRule.compare(betterHeader, worseHeader)).isPositive();
  }

  @Test
  public void identicalHeadersTie() {
    headerBuilder.number(8);

    headerBuilder.difficulty(Difficulty.of(1));
    final Block block = createEmptyBlock(keyPairs.get(1));

    final Comparator<BlockHeader> blockChoiceRule = blockchain.getBlockChoiceRule();

    // the only way to get a tie is to test the same block header
    //noinspection EqualsWithItself
    assertThat(blockChoiceRule.compare(block.getHeader(), block.getHeader())).isZero();
  }
}
