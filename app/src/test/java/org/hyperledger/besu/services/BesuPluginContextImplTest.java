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
package org.hyperledger.besu.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.plugin.services.BesuService;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BesuPluginContextImpl} covering service registry correctness, thread
 * safety, overwrite detection, and lifecycle diagnostics.
 */
public class BesuPluginContextImplTest {

  interface TestService extends BesuService {
    String id();
  }

  interface TestServiceA extends BesuService {}

  interface TestServiceB extends BesuService {}

  interface TestServiceC extends BesuService {}

  private BesuPluginContextImpl context;

  @BeforeEach
  void setUp() {
    context = new BesuPluginContextImpl();
    context.initialize(PluginConfiguration.DEFAULT);
  }

  // -------------------------------------------------------------------------
  // Basic add and get
  // -------------------------------------------------------------------------

  @Test
  void serviceRegistrySupportsBasicAddAndGet() {
    final TestServiceA serviceA = new TestServiceA() {};

    context.addService(TestServiceA.class, serviceA);

    final Optional<TestServiceA> retrieved = context.getService(TestServiceA.class);
    assertThat(retrieved).isPresent().contains(serviceA);
  }

  @Test
  void getServiceReturnsEmptyForUnregisteredService() {
    final Optional<TestServiceA> retrieved = context.getService(TestServiceA.class);
    assertThat(retrieved).isEmpty();
  }

  @Test
  void multipleServicesCanBeRegisteredAndRetrieved() {
    final TestServiceA serviceA = new TestServiceA() {};
    final TestServiceB serviceB = new TestServiceB() {};
    final TestServiceC serviceC = new TestServiceC() {};

    context.addService(TestServiceA.class, serviceA);
    context.addService(TestServiceB.class, serviceB);
    context.addService(TestServiceC.class, serviceC);

    assertThat(context.getService(TestServiceA.class)).isPresent().contains(serviceA);
    assertThat(context.getService(TestServiceB.class)).isPresent().contains(serviceB);
    assertThat(context.getService(TestServiceC.class)).isPresent().contains(serviceC);
  }

  // -------------------------------------------------------------------------
  // Overwrite detection
  // -------------------------------------------------------------------------

  @Test
  void addService_firstRegistration_succeeds() {
    final TestService svc = () -> "alpha";

    assertThatCode(() -> context.addService(TestService.class, svc)).doesNotThrowAnyException();
    assertThat(context.getService(TestService.class)).contains(svc);
  }

  @Test
  void addService_sameInstance_doesNotOverwrite() {
    final TestService svc = () -> "alpha";

    context.addService(TestService.class, svc);
    assertThatCode(() -> context.addService(TestService.class, svc)).doesNotThrowAnyException();
    assertThat(context.getService(TestService.class)).contains(svc);
  }

  @Test
  void addService_differentInstance_replacesAndServiceReturnsNew() {
    final TestService first = () -> "first";
    final TestService second = () -> "second";

    context.addService(TestService.class, first);
    context.addService(TestService.class, second);

    assertThat(context.getService(TestService.class)).contains(second);
  }

  // -------------------------------------------------------------------------
  // Missing service diagnostic
  // -------------------------------------------------------------------------

  @Test
  void getService_unregisteredService_returnsEmpty() {
    final Optional<TestService> result = context.getService(TestService.class);
    assertThat(result).isEmpty();
  }

  @Test
  void getService_afterRegistration_returnsService() {
    final TestService svc = () -> "beta";
    context.addService(TestService.class, svc);

    assertThat(context.getService(TestService.class)).contains(svc);
  }

  // -------------------------------------------------------------------------
  // Type validation
  // -------------------------------------------------------------------------

  @Test
  @SuppressWarnings("unchecked")
  void addService_withConcreteClass_throwsIllegalArgumentException() {
    final TestService svc = () -> "test";
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> context.addService((Class) String.class, svc))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Services must be Java interfaces");
  }

  // -------------------------------------------------------------------------
  // Thread safety — ConcurrentHashMap under concurrent load
  // -------------------------------------------------------------------------

  @Test
  void serviceRegistryHandlesConcurrentReadsAndWrites() throws Exception {
    final int threadCount = 10;
    final int operationsPerThread = 100;
    final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final AtomicBoolean failed = new AtomicBoolean(false);
    final List<Future<?>> futures = new ArrayList<>();

    final TestServiceA serviceA = new TestServiceA() {};
    context.addService(TestServiceA.class, serviceA);

    for (int i = 0; i < threadCount; i++) {
      final int threadIndex = i;
      futures.add(
          executor.submit(
              () -> {
                try {
                  startLatch.await();
                  for (int op = 0; op < operationsPerThread; op++) {
                    if (threadIndex % 2 == 0) {
                      context.addService(TestServiceB.class, new TestServiceB() {});
                    } else {
                      context.getService(TestServiceA.class);
                      context.getService(TestServiceB.class);
                    }
                  }
                } catch (final Exception e) {
                  failed.set(true);
                }
              }));
    }

    startLatch.countDown();

    for (final Future<?> future : futures) {
      future.get(10, TimeUnit.SECONDS);
    }

    executor.shutdown();
    assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    assertThat(failed.get()).isFalse();
    assertThat(context.getService(TestServiceA.class)).isPresent().contains(serviceA);
    assertThat(context.getService(TestServiceB.class)).isPresent();
  }
}
