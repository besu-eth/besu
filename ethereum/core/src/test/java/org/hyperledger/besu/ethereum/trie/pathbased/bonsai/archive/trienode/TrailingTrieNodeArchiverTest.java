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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TrailingTrieNodeArchiverTest {

  private final ArchiveChainFixture chain = new ArchiveChainFixture();
  private final TrailingTrieNodeArchiver archiver =
      new TrailingTrieNodeArchiver(
          chain.blockchain,
          chain.history,
          chain.newStep(),
          () -> Math.min(chain.finalThrough, chain.persistedHead()),
          MoreExecutors.newDirectExecutorService());

  @AfterEach
  void tearDown() {
    archiver.close();
    chain.close();
  }

  @Test
  void archivesUpToThePersistedHeadThenInlineCaptureContinues() {
    // BFT: every block is final, but capture started behind the head
    chain.finalThrough = 0;
    chain.persistGenesis();
    for (int i = 0; i < 3; i++) {
      chain.persistBusyBlock();
    }
    assertThat(chain.lastArchived()).isZero();
    chain.finalThrough = Long.MAX_VALUE;

    archiver.start();
    assertThat(chain.lastArchived()).isEqualTo(3);

    chain.persistBusyBlock();
    assertThat(chain.lastArchived()).isEqualTo(4);
    for (int b = 0; b <= 4; b++) {
      chain.assertArchived(b);
    }
  }

  @Test
  void inlineCaptureResumesAfterANetworkHeadDip() {
    chain.persistGenesis();
    chain.persistBusyBlock();
    chain.persistBusyBlock();
    assertThat(chain.lastArchived()).isEqualTo(2);

    // the network head dips, so the next blocks aren't final and inline capture pauses
    chain.finalThrough = 2;
    chain.persistBusyBlock();
    chain.persistEmptyBlock();
    archiver.archiveToTarget();
    assertThat(chain.lastArchived()).isEqualTo(2);

    chain.finalThrough = Long.MAX_VALUE;
    archiver.archiveToTarget();
    assertThat(chain.lastArchived()).isEqualTo(4);

    chain.persistBusyBlock();
    assertThat(chain.lastArchived()).isEqualTo(5);
    for (int b = 0; b <= 5; b++) {
      chain.assertArchived(b);
    }
  }

  @Test
  void trailsTheFinalizedBlock() {
    chain.finalThrough = 0;
    chain.persistGenesis();
    for (int i = 0; i < 5; i++) {
      chain.persistBusyBlock();
    }
    chain.finalThrough = 3;

    archiver.archiveToTarget();

    assertThat(chain.lastArchived()).isEqualTo(3);
    chain.assertArchived(3);
  }

  @Test
  void eachBlockIsWrittenOnceWhenInlineCaptureGetsThereFirst() {
    chain.persistGenesis();
    chain.persistBusyBlock();

    // inline capture has already archived the persisted head
    archiver.archiveToTarget();

    assertThat(chain.lastArchived()).isEqualTo(1);
    chain.assertArchived(1);
  }

  @Test
  void haltsWhenTheArchivedBlockIsNoLongerCanonical() {
    chain.persistGenesis();
    chain.persistBusyBlock();
    chain.finalThrough = 1;
    chain.persistBusyBlock();
    // a reorg replaces archived block 1
    chain.headers.set(
        1, new BlockHeaderTestFixture().number(1).parentHash(Hash.EMPTY).buildHeader());
    chain.finalThrough = Long.MAX_VALUE;

    archiver.start();

    assertThat(archiver.isHalted()).isTrue();
    assertThat(chain.lastArchived()).isEqualTo(1);
  }

  @Test
  void stopsThePassWhenTheChainNoLongerExtendsTheArchive() {
    chain.persistGenesis();
    chain.finalThrough = 0;
    chain.persistBusyBlock();
    chain.persistBusyBlock();
    // block 2 now belongs to a different chain than block 1
    chain.headers.set(
        2, new BlockHeaderTestFixture().number(2).parentHash(Hash.EMPTY).buildHeader());
    chain.finalThrough = Long.MAX_VALUE;

    archiver.archiveToTarget();

    assertThat(chain.lastArchived()).isEqualTo(1);
    assertThat(archiver.isHalted()).isFalse();
  }

  @Test
  void closeStopsObservingTheChain() {
    when(chain.blockchain.observeBlockAdded(any())).thenReturn(7L);
    archiver.start();

    archiver.close();

    verify(chain.blockchain).removeObserver(7L);
  }
}
