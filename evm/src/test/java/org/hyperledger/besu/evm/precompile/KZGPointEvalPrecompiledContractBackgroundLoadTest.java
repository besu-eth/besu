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
package org.hyperledger.besu.evm.precompile;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.precompile.KZGPointEvalPrecompileContractTest.PrecompileTestParameters;
import org.hyperledger.besu.evm.precompile.PrecompiledContract.PrecompileContractResult;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
import ethereum.ckzg4844.CKZG4844JNI;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Loading the trusted setup on the calling thread or in the background. */
class KZGPointEvalPrecompiledContractBackgroundLoadTest {
  private static void loadMainnetSetup() {
    CKZG4844JNI.loadTrustedSetupFromResource(
        "/kzg-trusted-setups/mainnet.txt", KZGPointEvalPrecompiledContract.class, 0);
  }

  private final KZGPointEvalPrecompiledContract contract = new KZGPointEvalPrecompiledContract();
  private final MessageFrame frame = mock(MessageFrame.class);
  private Bytes validInput;

  @BeforeAll
  static void loadNativeLibrary() {
    // once: each call loads another copy of the library, and the native methods may then bind to
    // different copies
    KZGPointEvalPrecompiledContract.loadLib();
  }

  @BeforeEach
  void setUp() throws IOException {
    // a cached result would skip the wait under test
    KZGPointEvalPrecompiledContract.setPrecompileCaching(false);
    validInput =
        KZGPointEvalPrecompileContractTest.getPointEvaluationPrecompileTestVectors().stream()
            .filter(PrecompileTestParameters::valid)
            .findFirst()
            .orElseThrow()
            .input();
  }

  @AfterEach
  void tearDown() {
    KZGPointEvalPrecompiledContract.tearDown();
  }

  @Test
  void loadsOnCallingThreadUnlessAskedToLoadInBackground() {
    final AtomicReference<Thread> loadingThread = new AtomicReference<>();

    KZGPointEvalPrecompiledContract.loadTrustedSetup(
        () -> {
          loadingThread.set(Thread.currentThread());
          loadMainnetSetup();
        },
        false);

    assertThat(loadingThread.get()).isSameAs(Thread.currentThread());
    assertThat(contract.computePrecompile(validInput, frame).state())
        .isEqualTo(MessageFrame.State.COMPLETED_SUCCESS);
  }

  @Test
  void kzgOperationsWaitForBackgroundLoad() throws Exception {
    final CountDownLatch releaseLoad = new CountDownLatch(1);
    final CountDownLatch callerStarted = new CountDownLatch(1);
    final AtomicReference<Thread> loadingThread = new AtomicReference<>();

    KZGPointEvalPrecompiledContract.loadTrustedSetup(
        () -> {
          loadingThread.set(Thread.currentThread());
          // bounded, so a load on the calling thread fails this test rather than hanging it
          assertThat(Uninterruptibles.awaitUninterruptibly(releaseLoad, 10, SECONDS)).isTrue();
          loadMainnetSetup();
        },
        true);
    final CompletableFuture<PrecompileContractResult> pointEvaluation =
        CompletableFuture.supplyAsync(
            () -> {
              callerStarted.countDown();
              return contract.computePrecompile(validInput, frame);
            });
    assertThat(callerStarted.await(10, SECONDS)).isTrue();

    // held back for as long as the load is
    assertThatThrownBy(() -> pointEvaluation.get(200, MILLISECONDS))
        .isInstanceOf(TimeoutException.class);

    releaseLoad.countDown();

    assertThat(pointEvaluation.get(10, SECONDS).state())
        .isEqualTo(MessageFrame.State.COMPLETED_SUCCESS);
    assertThat(loadingThread.get().getName()).isEqualTo("kzg-trusted-setup-loader");
  }

  @Test
  void failedBackgroundLoadFailsKzgOperations() {
    final IllegalArgumentException corrupt = new IllegalArgumentException("corrupt trusted setup");
    KZGPointEvalPrecompiledContract.loadTrustedSetup(
        () -> {
          throw corrupt;
        },
        true);

    assertThatThrownBy(KZGPointEvalPrecompiledContract::awaitTrustedSetup)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("KZG trusted setup failed to load: " + corrupt)
        .hasCause(corrupt);
    // rather than halting as an invalid proof would
    assertThatThrownBy(() -> contract.computePrecompile(validInput, frame))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("KZG trusted setup failed to load");
  }
}
