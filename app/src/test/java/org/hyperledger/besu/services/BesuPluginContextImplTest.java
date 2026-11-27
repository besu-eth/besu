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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.BesuService;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

public class BesuPluginContextImplTest {

  interface TestServiceA extends BesuService {}

  interface TestServiceB extends BesuService {}

  interface TestServiceC extends BesuService {}

  @Test
  void serviceRegistrySupportsBasicAddAndGet() {
    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    final TestServiceA serviceA = new TestServiceA() {};

    context.addService(TestServiceA.class, serviceA);

    final Optional<TestServiceA> retrieved = context.getService(TestServiceA.class);
    assertThat(retrieved).isPresent().contains(serviceA);
  }

  @Test
  void getServiceReturnsEmptyForUnregisteredService() {
    final BesuPluginContextImpl context = new BesuPluginContextImpl();

    final Optional<TestServiceA> retrieved = context.getService(TestServiceA.class);
    assertThat(retrieved).isEmpty();
  }

  @Test
  void serviceRegistryHandlesConcurrentReadsAndWrites() throws Exception {
    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    final int threadCount = 10;
    final int operationsPerThread = 100;
    final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final AtomicBoolean failed = new AtomicBoolean(false);
    final List<Future<?>> futures = new ArrayList<>();

    // Pre-register one service so readers have something to find
    final TestServiceA serviceA = new TestServiceA() {};
    context.addService(TestServiceA.class, serviceA);

    // Half the threads write services, half read services concurrently
    for (int i = 0; i < threadCount; i++) {
      final int threadIndex = i;
      futures.add(
          executor.submit(
              () -> {
                try {
                  startLatch.await();
                  for (int op = 0; op < operationsPerThread; op++) {
                    if (threadIndex % 2 == 0) {
                      // Writer thread: repeatedly overwrite services
                      context.addService(TestServiceB.class, new TestServiceB() {});
                    } else {
                      // Reader thread: concurrently read services
                      context.getService(TestServiceA.class);
                      context.getService(TestServiceB.class);
                    }
                  }
                } catch (final Exception e) {
                  failed.set(true);
                }
              }));
    }

    // Start all threads simultaneously
    startLatch.countDown();

    for (final Future<?> future : futures) {
      future.get(10, TimeUnit.SECONDS);
    }

    executor.shutdown();
    assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    assertThat(failed.get()).isFalse();

    // Verify services are still accessible after concurrent operations
    assertThat(context.getService(TestServiceA.class)).isPresent().contains(serviceA);
    assertThat(context.getService(TestServiceB.class)).isPresent();
  }

  @Test
  void stopPluginsAfterStartupFailureStopsOnlyStartedPlugins() throws Exception {
    // The JVM shutdown hook that calls stopPlugins() is registered before startPlugins() completes,
    // so a startup failure fires it while the context is only partially started. stopPlugins() must
    // still run instead of throwing, and must not call stop() on a plugin that never started.
    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.initialize(PluginConfiguration.DEFAULT); // continueOnPluginError defaults to false

    final RecordingPlugin startedPlugin = new RecordingPlugin(false);
    final RecordingPlugin failingPlugin = new RecordingPlugin(true);
    addRegisteredPlugin(context, startedPlugin);
    addRegisteredPlugin(context, failingPlugin);
    setState(context, "BEFORE_EXTERNAL_SERVICES_FINISHED");

    // With continueOnPluginError=false the failing plugin aborts startup, leaving the context in a
    // partially started state (BEFORE_MAIN_LOOP_STARTED).
    assertThatThrownBy(context::startPlugins).isInstanceOf(RuntimeException.class);

    assertThatCode(context::stopPlugins).doesNotThrowAnyException();
    assertThat(startedPlugin.stopCalled).isTrue();
    assertThat(failingPlugin.stopCalled).isFalse();
  }

  @Test
  void stopPluginsIsIdempotent() throws Exception {
    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.initialize(PluginConfiguration.DEFAULT);

    final RecordingPlugin plugin = new RecordingPlugin(false);
    addRegisteredPlugin(context, plugin);
    setState(context, "BEFORE_EXTERNAL_SERVICES_FINISHED");

    context.startPlugins();
    context.stopPlugins();
    assertThat(plugin.stopCount).isEqualTo(1);

    // A second call (for example a shutdown hook racing an explicit stop) is a no-op.
    assertThatCode(context::stopPlugins).doesNotThrowAnyException();
    assertThat(plugin.stopCount).isEqualTo(1);
  }

  private static void addRegisteredPlugin(
      final BesuPluginContextImpl context, final BesuPlugin plugin) throws Exception {
    final Field field = BesuPluginContextImpl.class.getDeclaredField("registeredPlugins");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    final List<BesuPlugin> registeredPlugins = (List<BesuPlugin>) field.get(context);
    registeredPlugins.add(plugin);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void setState(final BesuPluginContextImpl context, final String stateName)
      throws Exception {
    final Field field = BesuPluginContextImpl.class.getDeclaredField("state");
    field.setAccessible(true);
    field.set(
        context, Enum.valueOf((Class<Enum>) field.getType().asSubclass(Enum.class), stateName));
  }

  private static final class RecordingPlugin implements BesuPlugin {
    private final boolean failOnStart;
    private boolean stopCalled;
    private int stopCount;

    RecordingPlugin(final boolean failOnStart) {
      this.failOnStart = failOnStart;
    }

    @Override
    public void register(final ServiceManager context) {}

    @Override
    public void start() {
      if (failOnStart) {
        throw new RuntimeException("plugin start failed");
      }
    }

    @Override
    public void stop() {
      stopCalled = true;
      stopCount++;
    }
  }

  @Test
  void multipleServicesCanBeRegisteredAndRetrieved() {
    final BesuPluginContextImpl context = new BesuPluginContextImpl();
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
}
