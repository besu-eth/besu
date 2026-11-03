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
package org.hyperledger.besu.ethereum.chain;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.plugin.data.BadBlockCause.BadBlockReason;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

public class BadBlockManagerTest {

  final BlockchainSetupUtil chainUtil = BlockchainSetupUtil.forMainnet();
  final Block block = chainUtil.getBlock(1);
  final Block block2 = chainUtil.getBlock(2);
  final BadBlockManager badBlockManager = new BadBlockManager();

  @Test
  public void checkAndMarkBadDescendant_marksChildOfBadBlockAsBadHeader() {
    final Hash latestValidHash = Hash.fromHexStringLenient("0x1337");
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));
    badBlockManager.addLatestValidHash(block.getHash(), latestValidHash);

    assertThat(badBlockManager.checkAndMarkBadDescendant(block2.getHeader()))
        .map(BadBlockCause::getDescription)
        .contains("Descends from bad block " + block.getHeader().toLogString());
    assertThat(badBlockManager.getBadBlock(block2.getHash())).isEmpty();
    assertThat(badBlockManager.getBadHeader(block2.getHash())).contains(block2.getHeader());
    assertThat(badBlockManager.getLatestValidHash(block2.getHash())).contains(latestValidHash);
  }

  @Test
  public void checkAndMarkBadDescendant_marksChildOfBadHeaderAsBadHeader() {
    badBlockManager.addBadHeader(block.getHeader(), BadBlockCause.fromValidationFailure("failed"));

    assertThat(badBlockManager.checkAndMarkBadDescendant(block2.getHeader()))
        .map(BadBlockCause::getReason)
        .contains(BadBlockReason.DESCENDS_FROM_BAD_BLOCK);
    assertThat(badBlockManager.getBadHeader(block2.getHash())).contains(block2.getHeader());
    assertThat(badBlockManager.getLatestValidHash(block2.getHash())).isEmpty();
  }

  @Test
  public void checkAndMarkBadDescendant_doesNotReRecordKnownBadBlock() {
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));
    final BadBlockCause ownCause = BadBlockCause.fromValidationFailure("failed on its own");
    badBlockManager.addBadBlock(block2, ownCause);

    assertThat(badBlockManager.checkAndMarkBadDescendant(block2.getHeader())).contains(ownCause);
    assertThat(badBlockManager.getBadHeaders()).isEmpty();
  }

  @Test
  public void checkAndMarkBadDescendant_causeKeepsNamingTheRootAcrossDescendants() {
    final Block block3 = chainUtil.getBlock(3);
    final Block block4 = chainUtil.getBlock(4);
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));

    final BadBlockCause childCause =
        badBlockManager.checkAndMarkBadDescendant(block2.getHeader()).orElseThrow();
    final BadBlockCause grandChildCause =
        badBlockManager.checkAndMarkBadDescendant(block3.getHeader()).orElseThrow();
    final BadBlockCause greatGrandChildCause =
        badBlockManager.checkAndMarkBadDescendant(block4.getHeader()).orElseThrow();

    assertThat(childCause.getDescription())
        .isEqualTo("Descends from bad block " + block.getHeader().toLogString());
    assertThat(grandChildCause).isSameAs(childCause);
    assertThat(greatGrandChildCause).isSameAs(childCause);
    assertThat(badBlockManager.getBadBlockCause(block4.getHash())).contains(childCause);
  }

  @Test
  public void markBadChain_keepsTheBodyOfADescendantKnownAsBlock() {
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));

    badBlockManager.markBadChain(block.getHeader(), List.of(block2), List.of(), Optional.empty());

    assertThat(badBlockManager.getBadBlock(block2.getHash())).contains(block2);
    assertThat(badBlockManager.getBadBlockCause(block2.getHash()))
        .map(BadBlockCause::getReason)
        .contains(BadBlockReason.DESCENDS_FROM_BAD_BLOCK);
  }

  @Test
  public void removeBadBlock_forgetsTheBlockItsCauseAndItsLatestValidHash() {
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));
    badBlockManager.addLatestValidHash(block.getHash(), Hash.fromHexStringLenient("0x1337"));
    badBlockManager.addBadHeader(block2.getHeader(), BadBlockCause.fromValidationFailure("failed"));

    badBlockManager.removeBadBlock(block.getHash());
    badBlockManager.removeBadBlock(block2.getHash());

    assertThat(badBlockManager.isBadBlock(block.getHash())).isFalse();
    assertThat(badBlockManager.isBadBlock(block2.getHash())).isFalse();
    assertThat(badBlockManager.getBadBlockCause(block.getHash())).isEmpty();
    assertThat(badBlockManager.getLatestValidHash(block.getHash())).isEmpty();
    assertThat(badBlockManager.isEmpty()).isTrue();
  }

  @Test
  public void removeBadBlock_forgetsDescendantsMarkedOnItsAccountButNotOnesThatFailedThemselves() {
    final Block block3 = chainUtil.getBlock(3);
    final Block block4 = chainUtil.getBlock(4);
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("transient"));
    badBlockManager.markBadChain(block.getHeader(), List.of(block2), List.of(), Optional.empty());
    badBlockManager.checkAndMarkBadDescendant(block3.getHeader());
    badBlockManager.addBadBlock(block4, BadBlockCause.fromValidationFailure("failed itself"));

    badBlockManager.removeBadBlock(block.getHash());

    assertThat(badBlockManager.isBadBlock(block.getHash())).isFalse();
    assertThat(badBlockManager.isBadBlock(block2.getHash())).isFalse();
    assertThat(badBlockManager.isBadBlock(block3.getHash())).isFalse();
    assertThat(badBlockManager.getBadBlocks()).containsExactly(block4);
  }

  @Test
  public void removeBadBlock_keepsADescendantThatLaterFailedOnItsOwn() {
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("transient"));
    badBlockManager.checkAndMarkBadDescendant(block2.getHeader());
    final BadBlockCause ownCause = BadBlockCause.fromValidationFailure("failed itself");
    badBlockManager.addBadBlock(block2, ownCause);

    badBlockManager.removeBadBlock(block.getHash());

    assertThat(badBlockManager.getBadBlocks()).containsExactly(block2);
    assertThat(badBlockManager.getBadHeaders()).isEmpty();
    assertThat(badBlockManager.getBadBlockCause(block2.getHash())).contains(ownCause);
  }

  @Test
  public void removeBadBlock_forgetsTheDescendantsOfABlockThatIsNoLongerTracked() {
    final Block block3 = chainUtil.getBlock(3);
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("transient"));
    badBlockManager.markBadChain(
        block.getHeader(), List.of(), List.of(block2.getHeader()), Optional.empty());
    badBlockManager.checkAndMarkBadDescendant(block3.getHeader());
    // the root is evicted while its descendants are still tracked
    final BlockDataGenerator generator = new BlockDataGenerator();
    for (int i = 0; i < 2 * BadBlockManager.MAX_BAD_CHAIN_SIZE; i++) {
      badBlockManager.getBadHeader(block2.getHash());
      badBlockManager.getBadHeader(block3.getHash());
      badBlockManager.addBadBlock(generator.block(), BadBlockCause.fromValidationFailure("failed"));
    }
    assertThat(badBlockManager.isBadBlock(block.getHash())).isFalse();
    assertThat(badBlockManager.isBadBlock(block2.getHash())).isTrue();

    badBlockManager.removeBadBlock(block.getHash());

    assertThat(badBlockManager.isBadBlock(block2.getHash())).isFalse();
    assertThat(badBlockManager.isBadBlock(block3.getHash())).isFalse();
  }

  @Test
  public void markBadChain_keepsTheEntryOfADescendantThatFailedOnItsOwn() {
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));
    final BadBlockCause ownCause = BadBlockCause.fromValidationFailure("failed itself");
    badBlockManager.addBadBlock(block2, ownCause);

    badBlockManager.markBadChain(
        block.getHeader(), List.of(), List.of(block2.getHeader()), Optional.empty());

    assertThat(badBlockManager.getBadHeaders()).isEmpty();
    assertThat(badBlockManager.getBadBlockCause(block2.getHash())).contains(ownCause);
  }

  @Test
  public void markBadChain_keepsDescendantBodiesWithinTheBudgetAcrossCalls() {
    final BlockDataGenerator generator = new BlockDataGenerator();
    final Block failed = generator.block();
    badBlockManager.addBadBlock(failed, BadBlockCause.fromValidationFailure("failed"));

    for (int session = 0; session < 3; session++) {
      final List<Block> descendants =
          Stream.generate(generator::block)
              .limit(BadBlockManager.MAX_BAD_DESCENDANT_BODIES)
              .toList();
      badBlockManager.markBadChain(failed.getHeader(), descendants, List.of(), Optional.empty());
      descendants.forEach(
          descendant -> assertThat(badBlockManager.isBadBlock(descendant.getHash())).isTrue());
    }

    assertThat(badBlockManager.getBadBlocks())
        .hasSize(BadBlockManager.MAX_BAD_DESCENDANT_BODIES + 1)
        .contains(failed);
    assertThat(badBlockManager.getBadHeaders())
        .hasSize(2 * BadBlockManager.MAX_BAD_DESCENDANT_BODIES);
  }

  @Test
  public void addBadHeader_keepsTheEntryOfABlockWhoseBodyIsTracked() {
    final BadBlockCause ownCause = BadBlockCause.fromValidationFailure("failed");
    badBlockManager.addBadBlock(block, ownCause);

    badBlockManager.addBadHeader(block.getHeader(), BadBlockCause.fromValidationFailure("header"));

    assertThat(badBlockManager.getBadBlocks()).containsExactly(block);
    assertThat(badBlockManager.getBadHeaders()).isEmpty();
    assertThat(badBlockManager.getBadBlockCause(block.getHash())).contains(ownCause);
  }

  @Test
  public void markBadChain_marksNothingWhenTheRootIsNoLongerTracked() {
    assertThat(
            badBlockManager.markBadChain(
                block.getHeader(), List.of(), List.of(block2.getHeader()), Optional.empty()))
        .isFalse();
    assertThat(badBlockManager.isEmpty()).isTrue();
  }

  @Test
  public void removeBadBlock_leavesOtherBadBlocksAlone() {
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));
    badBlockManager.addBadBlock(block2, BadBlockCause.fromValidationFailure("failed"));

    badBlockManager.removeBadBlock(block.getHash());

    assertThat(badBlockManager.getBadBlocks()).containsExactly(block2);
  }

  @Test
  public void checkAndMarkBadDescendant_falseWhenNeitherBlockNorParentIsBad() {
    assertThat(badBlockManager.checkAndMarkBadDescendant(block2.getHeader())).isEmpty();
    assertThat(badBlockManager.getBadBlocks()).isEmpty();
    assertThat(badBlockManager.getBadHeaders()).isEmpty();
  }

  @Test
  public void addBadBlock_headerStaysDetectableAfterBodyEviction() {
    final BlockDataGenerator generator = new BlockDataGenerator();
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));

    for (int i = 0; i < BadBlockManager.MAX_BAD_BLOCKS_SIZE; i++) {
      badBlockManager.addBadBlock(generator.block(), BadBlockCause.fromValidationFailure("failed"));
    }

    assertThat(badBlockManager.getBadBlock(block.getHash())).isEmpty();
    assertThat(badBlockManager.isBadBlock(block.getHash())).isTrue();
    assertThat(badBlockManager.getBadHeader(block.getHash())).contains(block.getHeader());
  }

  @Test
  public void addBadBlock_addsBlock() {
    BadBlockManager badBlockManager = new BadBlockManager();
    final BadBlockCause cause = BadBlockCause.fromValidationFailure("failed");
    badBlockManager.addBadBlock(block, cause);

    assertThat(badBlockManager.getBadBlocks()).containsExactly(block);
  }

  @Test
  public void addBadBlockWithGeneratedBal_storesGeneratedAccessList() {
    BadBlockManager badBlockManager = new BadBlockManager();
    final BadBlockCause cause = BadBlockCause.fromValidationFailure("failed");
    final BlockAccessList generatedBal =
        new BlockAccessList(
            List.of(
                new BlockAccessList.AccountChanges(
                    Address.fromHexString("0x0000000000000000000000000000000000000002"),
                    List.of(),
                    List.of(new BlockAccessList.SlotRead(new StorageSlotKey(UInt256.valueOf(2)))),
                    List.of(),
                    List.of(),
                    List.of())));

    badBlockManager.addBadBlock(block, cause, Optional.empty(), Optional.of(generatedBal));

    assertThat(badBlockManager.getGeneratedBlockAccessList(block.getHash())).contains(generatedBal);
  }

  @Test
  public void addBadBlock_oversizedBlockAccessListIsNotRetained() {
    final BlockDataGenerator generator = new BlockDataGenerator();
    final Block badBlock = generator.block();
    // a single account whose code change alone is larger than the whole retention budget: one
    // item, but many bytes. Decoded from its encoding, as a list received with a block is.
    final Bytes code = Bytes.wrap(new byte[(int) BadBlockManager.MAX_BAD_BLOCK_ACCESS_LIST_BYTES]);
    final BlockAccessList oversized = BlockAccessList.fromBytes(balWithCodeChange(code).encode());
    assertThat(oversized.eip7928ItemCount()).isEqualTo(1);

    badBlockManager.addBadBlock(
        badBlock,
        BadBlockCause.fromValidationFailure("block access list over budget"),
        Optional.of(oversized),
        Optional.empty());

    // the block itself stays tracked as bad, so detection and the debug RPCs still work
    assertThat(badBlockManager.isBadBlock(badBlock.getHash())).isTrue();
    // but its attacker-sized access list is evicted, so a stream of them cannot exhaust the heap
    assertThat(badBlockManager.getBlockAccessList(badBlock.getHash())).isEmpty();
  }

  @Test
  public void addBadBlock_generatedBlockAccessListRetentionIsBoundedByEncodedSize() {
    final BlockDataGenerator generator = new BlockDataGenerator();
    // each list is a single item but a quarter of the budget in bytes, so only a few fit at once;
    // weighing by entry or item count would have kept all of them. Built in memory, as a list
    // generated by executing the block is, so it has no raw encoding and is weighed by encoding it.
    final Bytes code =
        Bytes.wrap(new byte[(int) (BadBlockManager.MAX_BAD_BLOCK_ACCESS_LIST_BYTES / 4)]);
    final int added = 8;

    final List<Hash> hashes = new ArrayList<>();
    for (int i = 0; i < added; i++) {
      final Block badBlock = generator.block();
      hashes.add(badBlock.getHash());
      badBlockManager.addBadBlock(
          badBlock,
          BadBlockCause.fromValidationFailure("block access list mismatch"),
          Optional.empty(),
          Optional.of(balWithCodeChange(code)));
    }

    final long retained =
        hashes.stream()
            .filter(hash -> badBlockManager.getGeneratedBlockAccessList(hash).isPresent())
            .count();
    // the cache shed older lists to stay within budget rather than retaining every entry
    assertThat(retained).isLessThan(added);
    // eviction is least-recently-used, so the earliest list is gone and the latest is kept
    assertThat(badBlockManager.getGeneratedBlockAccessList(hashes.get(0))).isEmpty();
    assertThat(badBlockManager.getGeneratedBlockAccessList(hashes.get(added - 1))).isPresent();
  }

  private static BlockAccessList balWithCodeChange(final Bytes code) {
    return new BlockAccessList(
        List.of(
            new BlockAccessList.AccountChanges(
                Address.fromHexString("0x0000000000000000000000000000000000000002"),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(new BlockAccessList.CodeChange(0, code)))));
  }

  @Test
  public void reset_clearsBadBlocks() {
    BadBlockManager badBlockManager = new BadBlockManager();
    final BadBlockCause cause = BadBlockCause.fromValidationFailure("");
    badBlockManager.addBadBlock(block, cause);
    assertThat(badBlockManager.getBadBlocks()).containsExactly(block);

    badBlockManager.reset();

    assertThat(badBlockManager.getBadBlocks()).isEmpty();
    assertThat(badBlockManager.getBadBlockCause(block.getHash())).isEmpty();
  }

  @Test
  public void isBadBlock_falseWhenEmpty() {
    assertThat(badBlockManager.isBadBlock(block.getHash())).isFalse();
  }

  @Test
  public void isBadBlock_trueForBadHeader() {
    badBlockManager.addBadHeader(block.getHeader(), BadBlockCause.fromValidationFailure("failed"));
    assertThat(badBlockManager.isBadBlock(block.getHash())).isTrue();
  }

  @Test
  public void isBadBlock_trueForBadBlock() {
    badBlockManager.addBadBlock(block, BadBlockCause.fromValidationFailure("failed"));

    assertThat(badBlockManager.isBadBlock(block.getHash())).isTrue();
  }

  @Test
  public void subscribeToBadBlocks_listenerReceivesBadBlockEvent() {

    final AtomicReference<org.hyperledger.besu.plugin.data.BlockHeader> badBlockResult =
        new AtomicReference<>();
    final AtomicReference<org.hyperledger.besu.plugin.data.BadBlockCause> badBlockCauseResult =
        new AtomicReference<>();

    badBlockManager.subscribeToBadBlocks(
        (badBlock, cause) -> {
          badBlockResult.set(badBlock);
          badBlockCauseResult.set(cause);
        });

    final BadBlockCause cause = BadBlockCause.fromValidationFailure("fail");
    badBlockManager.addBadBlock(block, cause);

    // Check event was emitted
    assertThat(badBlockResult.get()).isEqualTo(block.getHeader());
    assertThat(badBlockCauseResult.get()).isEqualTo(cause);
  }

  @Test
  public void subscribeToBadBlocks_listenerReceivesBadHeaderEvent() {

    final AtomicReference<org.hyperledger.besu.plugin.data.BlockHeader> badBlockResult =
        new AtomicReference<>();
    final AtomicReference<org.hyperledger.besu.plugin.data.BadBlockCause> badBlockCauseResult =
        new AtomicReference<>();

    badBlockManager.subscribeToBadBlocks(
        (badBlock, cause) -> {
          badBlockResult.set(badBlock);
          badBlockCauseResult.set(cause);
        });

    final BadBlockCause cause = BadBlockCause.fromValidationFailure("fail");
    badBlockManager.addBadHeader(block.getHeader(), cause);

    // Check event was emitted
    assertThat(badBlockResult.get()).isEqualTo(block.getHeader());
    assertThat(badBlockCauseResult.get()).isEqualTo(cause);
  }

  @Test
  public void unsubscribeFromBadBlocks_listenerReceivesNoEvents() {

    final AtomicInteger eventCount = new AtomicInteger(0);
    final long subscribeId =
        badBlockManager.subscribeToBadBlocks((block, cause) -> eventCount.incrementAndGet());
    badBlockManager.unsubscribeFromBadBlocks(subscribeId);

    final BadBlockCause cause = BadBlockCause.fromValidationFailure("fail");
    badBlockManager.addBadBlock(block, cause);
    badBlockManager.addBadHeader(block2.getHeader(), cause);

    // Check no events fired
    assertThat(eventCount.get()).isEqualTo(0);
  }
}
