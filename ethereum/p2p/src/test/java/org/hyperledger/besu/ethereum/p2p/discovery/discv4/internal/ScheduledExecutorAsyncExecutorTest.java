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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ScheduledExecutorAsyncExecutorTest {

  private ExecutorService executor;
  private ScheduledExecutorAsyncExecutor asyncExecutor;

  @BeforeEach
  public void setUp() {
    executor = Executors.newSingleThreadExecutor();
    asyncExecutor = new ScheduledExecutorAsyncExecutor(executor);
  }

  @AfterEach
  public void tearDown() {
    executor.shutdownNow();
  }

  @Test
  public void execute_completesWithActionResult() throws Exception {
    final CompletableFuture<String> result = asyncExecutor.execute(() -> "done");

    assertThat(result.get()).isEqualTo("done");
  }

  @Test
  public void execute_completesExceptionallyWhenActionThrows() {
    final CompletableFuture<String> result =
        asyncExecutor.execute(
            () -> {
              throw new IllegalStateException("action failed");
            });

    assertThatCode(result::get)
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(IllegalStateException.class);
  }

  @Test
  public void execute_afterShutdown_completesExceptionallyInsteadOfThrowing() {
    executor.shutdownNow();

    final CompletableFuture<String> result = asyncExecutor.execute(() -> "unreachable");

    assertThat(result).isCompletedExceptionally();
    assertThatCode(result::get)
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(RejectedExecutionException.class);
  }
}
