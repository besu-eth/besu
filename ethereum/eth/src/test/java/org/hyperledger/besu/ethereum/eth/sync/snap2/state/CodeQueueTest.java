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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Format;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.util.List;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class CodeQueueTest {

  private final SyncStateStorage storage = SyncStorageFixture.create(Format.BONSAI).sync();
  private final CodeQueue queue = new CodeQueue(storage);

  @Test
  void takesTheBytecodesInTheOrderTheyCameABatchAtATime() {
    for (int i = 1; i <= 5; i++) {
      queue.add(hash(i));
    }
    queue.add(hash(1));

    assertThat(queue.size()).isEqualTo(5);
    assertThat(queue.take(3)).containsExactly(hash(1), hash(2), hash(3));
    assertThat(queue.take(3)).containsExactly(hash(4), hash(5));
    assertThat(queue.isEmpty()).isTrue();
  }

  @Test
  void dropsTheBytecodesAlreadyStored() {
    final SyncStateStorage.Batch batch = storage.batch();
    batch.putCode(hash(2), Bytes.of(2));
    batch.commit();
    queue.add(hash(1));
    queue.add(hash(2));
    queue.add(hash(3));

    assertThat(queue.take(2)).containsExactly(hash(1), hash(3));
    assertThat(queue.isEmpty()).isTrue();
  }

  // a failed lookup only costs a download
  @Test
  void asksForABytecodeWhoseLookupFailed() {
    final SyncStateStorage failing = mock(SyncStateStorage.class);
    when(failing.hasCode(any())).thenThrow(new StorageException("disk failure"));
    final CodeQueue failingQueue = new CodeQueue(failing);
    failingQueue.add(hash(1));

    assertThat(failingQueue.take(1)).containsExactly(hash(1));
  }

  @Test
  void queuesAgainTheBytecodesAnAnswerDidNotStore() {
    queue.add(hash(1));
    queue.add(hash(2));
    final List<Bytes32> asked = queue.take(2);

    queue.answered(asked, Set.of(hash(1)));

    assertThat(queue.take(2)).containsExactly(hash(2));
  }

  @Test
  void ignoresASecondAnswerOfTheSameBatch() {
    queue.add(hash(1));
    final List<Bytes32> asked = queue.take(1);
    queue.answered(asked, Set.of(hash(1)));

    queue.answered(asked, Set.of());

    assertThat(queue.isEmpty()).isTrue();
  }

  // a bytecode shared by many accounts is asked once
  @Test
  void ignoresABytecodeQueuedLately() {
    queue.add(hash(1));
    queue.answered(queue.take(1), Set.of(hash(1)));

    queue.add(hash(1));

    assertThat(queue.isEmpty()).isTrue();
  }

  // the bytecodes seen are bounded: past the bound they are forgotten, not those in flight
  @Test
  void forgetsTheBytecodesSeenPastItsBound() {
    queue.add(hash(1));
    queue.answered(queue.take(1), Set.of(hash(1)));
    queue.add(hash(2));
    final List<Bytes32> inFlight = queue.take(1);
    for (int i = 3; queue.size() < CodeQueue.SEEN_LIMIT; i++) {
      queue.add(hash(i));
    }
    queue.take(CodeQueue.SEEN_LIMIT);

    queue.add(hash(1));
    queue.add(hash(2));

    assertThat(inFlight).containsExactly(hash(2));
    assertThat(queue.take(2)).containsExactly(hash(1));
  }

  private static Bytes32 hash(final int value) {
    return Bytes32.leftPad(Bytes.ofUnsignedInt(value));
  }
}
