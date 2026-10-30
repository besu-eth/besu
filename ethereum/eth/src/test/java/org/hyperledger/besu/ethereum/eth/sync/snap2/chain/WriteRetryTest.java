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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.chain.StorageFailures.diskFailure;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.chain.StorageFailures.notCommitted;

import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class WriteRetryTest {

  private final List<Long> pauses = new ArrayList<>();
  private final AtomicInteger tries = new AtomicInteger();

  @Test
  void triesAWriteAgainUntilItCommits() {
    WriteRetry.run(failing(2, notCommitted()), pauses::add);

    assertThat(tries).hasValue(3);
    assertThat(pauses).containsExactly(20L, 40L);
  }

  @Test
  void givesUpAfterSixTries() {
    assertThatThrownBy(() -> WriteRetry.run(failing(6, notCommitted()), pauses::add))
        .isInstanceOf(StorageException.class);

    assertThat(tries).hasValue(6);
    assertThat(pauses).containsExactly(20L, 40L, 80L, 160L, 320L);
  }

  @Test
  void triesAStorageFailureOnce() {
    assertThatThrownBy(() -> WriteRetry.run(failing(1, diskFailure()), pauses::add))
        .isInstanceOf(StorageException.class);

    assertThat(tries).hasValue(1);
    assertThat(pauses).isEmpty();
  }

  @Test
  void triesAFailureWithoutACauseOnce() {
    final StorageException closed = new StorageException("storage closed");

    assertThatThrownBy(() -> WriteRetry.run(failing(1, closed), pauses::add)).isSameAs(closed);
    assertThat(tries).hasValue(1);
  }

  @Test
  void stopsOnAnInterruptedPause() {
    assertThatThrownBy(
            () ->
                WriteRetry.run(
                    failing(1, notCommitted()),
                    millis -> {
                      throw new InterruptedException();
                    }))
        .isInstanceOf(CancellationException.class);

    assertThat(tries).hasValue(1);
    // left for the caller to see
    assertThat(Thread.interrupted()).isTrue();
  }

  // a write that fails this many times, then commits
  private Runnable failing(final int failures, final StorageException failure) {
    return () -> {
      if (tries.incrementAndGet() <= failures) {
        throw failure;
      }
    };
  }
}
