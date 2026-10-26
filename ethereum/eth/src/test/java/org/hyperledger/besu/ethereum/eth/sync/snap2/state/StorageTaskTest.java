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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.trie.RangeManager;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class StorageTaskTest {

  private static final Bytes32 MIDDLE = Bytes32.fromHexString("0x80" + "00".repeat(31));
  private static final Bytes32 ROOT = Bytes32.fromHexString("0x" + "11".repeat(32));
  private static final Bytes32 OTHER_ROOT = Bytes32.fromHexString("0x" + "22".repeat(32));
  private static final Bytes ROOT_NODE = Bytes.of(1, 2, 3);

  @Test
  void reportsTheCompletionOfTheStorageOnce() {
    final StorageTask task = new StorageTask(Bytes32.ZERO, Bytes32.ZERO);

    assertThat(task.cover(MIDDLE, RangeManager.MAX_RANGE, 3)).isFalse();
    // a range covered again, as a retried request's is, counts nothing more
    assertThat(task.cover(MIDDLE, RangeManager.MAX_RANGE, 3)).isFalse();
    assertThat(task.cover(RangeManager.MIN_RANGE, MIDDLE, 2)).isTrue();
    assertThat(task.cover(RangeManager.MIN_RANGE, MIDDLE, 2)).isFalse();
    assertThat(task.covers(MIDDLE)).isTrue();
    assertThat(task.slots()).isEqualTo(5);
  }

  @Test
  void givesTheRootNodeOfATrieWhoseRangesCameAtOneRoot() {
    final StorageTask task = new StorageTask(Bytes32.ZERO, ROOT);

    assertThat(task.writesTrieAt(ROOT)).isTrue();
    task.trieWritten(Optional.of(ROOT_NODE));
    assertThat(task.writesTrieAt(ROOT)).isTrue();
    task.trieWritten(Optional.of(ROOT_NODE));

    assertThat(task.trieRootNode()).contains(ROOT_NODE);
  }

  @Test
  void givesUpTheTrieOnceARangeComesAtAnotherRoot() {
    final StorageTask task = new StorageTask(Bytes32.ZERO, ROOT);
    assertThat(task.writesTrieAt(ROOT)).isTrue();
    task.trieWritten(Optional.of(ROOT_NODE));

    task.refreshRoot(OTHER_ROOT);
    assertThat(task.storageRoot()).isEqualTo(OTHER_ROOT);
    // no root node for a trie written at an older root
    assertThat(task.trieRootNode()).isEmpty();
    assertThat(task.writesTrieAt(OTHER_ROOT)).isFalse();
    // the ranges of the other root are missing from the trie, even if the root comes back
    task.refreshRoot(ROOT);
    assertThat(task.writesTrieAt(ROOT)).isFalse();
    assertThat(task.trieRootNode()).isEmpty();
  }

  @Test
  void hidesTheRootNodeWhileTheRootIsStale() {
    final StorageTask task = new StorageTask(Bytes32.ZERO, ROOT);
    assertThat(task.writesTrieAt(ROOT)).isTrue();
    task.trieWritten(Optional.of(ROOT_NODE));

    task.markRootStale();
    assertThat(task.isRootStale()).isTrue();
    assertThat(task.storageRoot()).isEqualTo(ROOT);
    assertThat(task.trieRootNode()).isEmpty();

    // the storage a move changed is back at the same root
    task.refreshRoot(ROOT);
    assertThat(task.isRootStale()).isFalse();
    assertThat(task.trieRootNode()).contains(ROOT_NODE);
  }

  @Test
  void givesUpTheTrieOfARangeThatGivesNoRootNode() {
    final StorageTask task = new StorageTask(Bytes32.ZERO, ROOT);
    assertThat(task.writesTrieAt(ROOT)).isTrue();

    task.trieWritten(Optional.empty());

    assertThat(task.writesTrieAt(ROOT)).isFalse();
    assertThat(task.trieRootNode()).isEmpty();
  }
}
