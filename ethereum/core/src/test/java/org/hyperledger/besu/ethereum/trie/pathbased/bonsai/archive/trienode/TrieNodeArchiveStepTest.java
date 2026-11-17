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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode.TrieNodeArchiveStep.UnarchivableBlockException;

import java.util.Optional;
import java.util.concurrent.Executors;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TrieNodeArchiveStepTest {

  private final ArchiveChainFixture chain = new ArchiveChainFixture();
  private final TrieNodeArchiveStep step = chain.newStep();

  @AfterEach
  void tearDown() {
    step.close();
    chain.close();
  }

  @Test
  void replayedBlocksMatchTheirStateRoots() {
    chain.finalThrough = 0;
    chain.persistGenesis();
    for (int i = 0; i < 6; i++) {
      chain.persistBusyBlock();
    }
    chain.persistEmptyBlock();
    chain.persistBusyBlock();
    assertThat(chain.lastArchived()).isZero();

    for (int b = 1; b <= chain.persistedHead(); b++) {
      assertThat(step.archive(chain.headers.get(b - 1), chain.headers.get(b))).isTrue();
    }

    assertThat(chain.lastArchived()).isEqualTo(chain.persistedHead());
    for (int b = 0; b <= chain.persistedHead(); b++) {
      chain.assertArchived(b);
    }
  }

  @Test
  void replayedNodesDiffAgainstTheParent() {
    chain.finalThrough = 0;
    chain.persistGenesis();
    chain.persistBusyBlock();

    step.archive(chain.headers.get(0), chain.headers.get(1));

    // the account trie's root branch changed, and diffs against block 0's
    assertThat(
            chain
                .history
                .getLatestBefore(ArchiveNodeKey.account(Bytes.EMPTY), 1L)
                .map(ArchiveNodeHistoryStore.HistoryEntry::counter))
        .contains(1);
  }

  @Test
  void blockInlineCaptureAlreadyArchivedIsDropped() {
    chain.persistGenesis();
    chain.persistBusyBlock();
    chain.persistBusyBlock();
    assertThat(chain.lastArchived()).isEqualTo(2);
    final Optional<Bytes> before = rootEntryAt(2L);

    // the archiver read block 1 as the last archived, then inline capture wrote block 2
    assertThat(step.archive(chain.headers.get(1), chain.headers.get(2))).isFalse();

    assertThat(chain.lastArchived()).isEqualTo(2);
    assertThat(rootEntryAt(2L)).isEqualTo(before);
  }

  private Optional<Bytes> rootEntryAt(final long block) {
    return chain
        .history
        .getLatestBefore(ArchiveNodeKey.account(Bytes.EMPTY), block)
        .map(ArchiveNodeHistoryStore.HistoryEntry::rawEntryBytes);
  }

  @Test
  void wrongStateRootIsUnarchivable() {
    chain.finalThrough = 0;
    chain.persistGenesis();
    final BlockHeader block1 = chain.persistBusyBlock();
    final BlockHeader wrongRoot =
        new BlockHeaderTestFixture()
            .number(1)
            .parentHash(block1.getParentHash())
            .stateRoot(Hash.EMPTY_TRIE_HASH)
            .buildHeader();
    final TrieNodeArchiveStep wrongRootStep =
        new TrieNodeArchiveStep(
            chain.liveStorage,
            chain.history,
            chain.writer,
            hash -> chain.provider.getTrieLogManager().getTrieLogLayer(block1.getHash()),
            Executors.newFixedThreadPool(1),
            1024);

    assertThatThrownBy(() -> wrongRootStep.archive(chain.headers.get(0), wrongRoot))
        .isInstanceOf(UnarchivableBlockException.class)
        .hasMessageContaining("replays to state root");
    assertThat(chain.lastArchived()).isZero();
    wrongRootStep.close();
  }

  @Test
  void missingTrieLogIsUnarchivable() {
    chain.finalThrough = 0;
    chain.persistGenesis();
    chain.persistBusyBlock();
    final TrieNodeArchiveStep noTrieLogs =
        new TrieNodeArchiveStep(
            chain.liveStorage,
            chain.history,
            chain.writer,
            hash -> Optional.empty(),
            Executors.newFixedThreadPool(1),
            1024);

    assertThatThrownBy(() -> noTrieLogs.archive(chain.headers.get(0), chain.headers.get(1)))
        .isInstanceOf(UnarchivableBlockException.class)
        .hasMessageContaining("no trie log");
    assertThat(chain.lastArchived()).isZero();
    noTrieLogs.close();
  }
}
