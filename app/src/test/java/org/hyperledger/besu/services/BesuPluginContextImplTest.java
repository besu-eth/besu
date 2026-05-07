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

import org.hyperledger.besu.ethereum.core.plugins.ImmutablePluginConfiguration;
import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.ServiceManager;
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
import java.util.concurrent.atomic.AtomicInteger;

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

  // -------------------------------------------------------------------------
  // Helpers shared by stopPlugins() lifecycle tests
  // -------------------------------------------------------------------------

  /**
   * Builds a PluginConfiguration that has external plugins disabled so that initialize() +
   * registerPlugins() succeed without touching the filesystem, and continueOnPluginError controls
   * whether a throwing plugin aborts startup.
   */
  private static PluginConfiguration configWith(final boolean continueOnPluginError) {
    return ImmutablePluginConfiguration.builder()
        .externalPluginsEnabled(false)
        .continueOnPluginError(continueOnPluginError)
        .build();
  }

  // -------------------------------------------------------------------------
  // stopPlugins() resilience tests  (P20 fix)
  // -------------------------------------------------------------------------

  /**
   * Core happy-path: after a full startup, stopPlugins() calls stop() on every plugin and does not
   * throw.
   */
  @Test
  void stopPlugins_callsStop_afterFullStartup() {
    final AtomicInteger stopCount = new AtomicInteger(0);
    final BesuPlugin plugin =
        new NoOpPlugin() {
          @Override
          public void stop() {
            stopCount.incrementAndGet();
          }
        };

    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.initialize(configWith(false));
    context.registerPlugins();
    context.addPluginForTesting(plugin);

    context.beforeExternalServices();
    context.startPlugins();

    assertThatCode(context::stopPlugins).doesNotThrowAnyException();
    assertThat(stopCount.get()).isEqualTo(1);
  }

  /**
   * Scenario A (the exact production crash path): shutdown hook fires while the context is in
   * BEFORE_MAIN_LOOP_STARTED because startPlugins() threw with continueOnPluginError=false.
   * stopPlugins() must not throw and must still call stop() on the plugin that successfully
   * completed start().
   */
  @Test
  void stopPlugins_doesNotThrow_whenStartPluginsFailedWithContinueOnErrorFalse() {
    final AtomicInteger stopCount = new AtomicInteger(0);

    // pluginA completes start() successfully
    final BesuPlugin pluginA =
        new NoOpPlugin() {
          @Override
          public void stop() {
            stopCount.incrementAndGet();
          }
        };
    // pluginB throws during start(), aborting the startup loop
    final BesuPlugin pluginB =
        new NoOpPlugin() {
          @Override
          public void start() {
            throw new RuntimeException("pluginB start failure");
          }

          @Override
          public void stop() {
            stopCount.incrementAndGet();
          }
        };

    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.initialize(configWith(false)); // continueOnPluginError = false
    context.registerPlugins();
    context.addPluginForTesting(pluginA);
    context.addPluginForTesting(pluginB);

    context.beforeExternalServices();

    // startPlugins() throws because pluginB fails and continueOnPluginError=false
    assertThatCode(context::startPlugins).isInstanceOf(RuntimeException.class);

    // The state is now BEFORE_MAIN_LOOP_STARTED — the old code crashed here
    assertThatCode(context::stopPlugins).doesNotThrowAnyException();

    // pluginA reached start() → must be stopped; pluginB never completed start() → skipped
    assertThat(stopCount.get()).isEqualTo(1);
  }

  /**
   * Scenario C: the JVM shuts down after startExternalServices() threw but after buildRunner()
   * already registered the shutdown hook. Context state = BEFORE_EXTERNAL_SERVICES_FINISHED. No
   * plugin ever reached start() so none should have stop() called.
   */
  @Test
  void stopPlugins_doesNotThrow_whenNoPluginReachedStart() {
    final AtomicInteger stopCount = new AtomicInteger(0);
    final BesuPlugin plugin =
        new NoOpPlugin() {
          @Override
          public void stop() {
            stopCount.incrementAndGet();
          }
        };

    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.initialize(configWith(false));
    context.registerPlugins();
    context.addPluginForTesting(plugin);

    // Only drive up to beforeExternalServices — startPlugins() never called
    context.beforeExternalServices();

    assertThatCode(context::stopPlugins).doesNotThrowAnyException();
    // Plugin never started → stop() must NOT be called
    assertThat(stopCount.get()).isEqualTo(0);
  }

  /**
   * stopPlugins() called before any lifecycle method has run (e.g. ThreadBesuNodeRunner calling
   * stopNode on a context that never completed registration). Must be a no-op — no throw, no stop()
   * calls.
   */
  @Test
  void stopPlugins_doesNotThrow_whenCalledBeforeAnyLifecycleStep() {
    final AtomicInteger stopCount = new AtomicInteger(0);
    final BesuPlugin plugin =
        new NoOpPlugin() {
          @Override
          public void stop() {
            stopCount.incrementAndGet();
          }
        };

    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.initialize(configWith(false));
    context.registerPlugins();
    context.addPluginForTesting(plugin);

    // Intentionally skip beforeExternalServices() and startPlugins()
    assertThatCode(context::stopPlugins).doesNotThrowAnyException();
    assertThat(stopCount.get()).isEqualTo(0);
  }

  /**
   * Idempotency: calling stopPlugins() a second time after a successful stop must be a silent no-op
   * — not throw, not call stop() again.
   */
  @Test
  void stopPlugins_isIdempotent_secondCallIsNoOp() {
    final AtomicInteger stopCount = new AtomicInteger(0);
    final BesuPlugin plugin =
        new NoOpPlugin() {
          @Override
          public void stop() {
            stopCount.incrementAndGet();
          }
        };

    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.initialize(configWith(false));
    context.registerPlugins();
    context.addPluginForTesting(plugin);
    context.beforeExternalServices();
    context.startPlugins();

    context.stopPlugins(); // first call — normal stop
    assertThatCode(context::stopPlugins).doesNotThrowAnyException(); // second call — must be no-op
    assertThat(stopCount.get()).isEqualTo(1); // stop() called exactly once
  }

  /**
   * stopPlugins() is re-entrant-safe: if the context is currently mid-stop (state == STOPPING), a
   * concurrent call must return immediately without calling stop() a second time.
   */
  @Test
  void stopPlugins_doesNotThrow_whenAlreadyStopping() throws Exception {
    final AtomicInteger stopCount = new AtomicInteger(0);
    final CountDownLatch stopEntered = new CountDownLatch(1);
    final CountDownLatch stopRelease = new CountDownLatch(1);

    final BesuPlugin slowPlugin =
        new NoOpPlugin() {
          @Override
          public void stop() {
            stopCount.incrementAndGet();
            stopEntered.countDown(); // signal that we are inside stop()
            try {
              stopRelease.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        };

    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.initialize(configWith(false));
    context.registerPlugins();
    context.addPluginForTesting(slowPlugin);
    context.beforeExternalServices();
    context.startPlugins();

    // First call in background — will block inside stop()
    final Thread firstStop = new Thread(context::stopPlugins, "first-stop");
    firstStop.start();
    assertThat(stopEntered.await(5, TimeUnit.SECONDS)).isTrue();

    // Second call while first is still running — must return immediately without throwing
    assertThatCode(context::stopPlugins).doesNotThrowAnyException();
    assertThat(stopCount.get()).isEqualTo(1); // stop() called only once so far

    stopRelease.countDown(); // unblock the first stop
    firstStop.join(5_000);
  }

  /**
   * With continueOnPluginError=true, a plugin that throws during start() is removed from
   * registeredPlugins before stopPlugins() is called. The remaining plugins that started
   * successfully must each receive stop().
   */
  @Test
  void stopPlugins_stopsOnlySuccessfullyStartedPlugins_withContinueOnErrorTrue() {
    final AtomicInteger stopCount = new AtomicInteger(0);

    final BesuPlugin pluginA =
        new NoOpPlugin() {
          @Override
          public void stop() {
            stopCount.incrementAndGet();
          }
        };
    final BesuPlugin pluginB =
        new NoOpPlugin() {
          @Override
          public void start() {
            throw new RuntimeException("pluginB start failure");
          }

          @Override
          public void stop() {
            stopCount.incrementAndGet();
          }
        };
    final BesuPlugin pluginC =
        new NoOpPlugin() {
          @Override
          public void stop() {
            stopCount.incrementAndGet();
          }
        };

    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.initialize(configWith(true)); // continueOnPluginError = true
    context.registerPlugins();
    context.addPluginForTesting(pluginA);
    context.addPluginForTesting(pluginB);
    context.addPluginForTesting(pluginC);

    context.beforeExternalServices();
    // startPlugins() removes pluginB from registeredPlugins when it throws
    assertThatCode(context::startPlugins).doesNotThrowAnyException();
    assertThatCode(context::stopPlugins).doesNotThrowAnyException();

    // pluginA and pluginC started and must be stopped; pluginB was removed so not stopped
    assertThat(stopCount.get()).isEqualTo(2);
  }

  /**
   * A plugin whose stop() throws must not prevent subsequent plugins from having their own stop()
   * called (stop errors are swallowed, not propagated).
   */
  @Test
  void stopPlugins_continuesStoppingRemainingPlugins_whenOneStopThrows() {
    final AtomicInteger stopCount = new AtomicInteger(0);

    final BesuPlugin pluginA =
        new NoOpPlugin() {
          @Override
          public void stop() {
            throw new RuntimeException("stop failure in A");
          }
        };
    final BesuPlugin pluginB =
        new NoOpPlugin() {
          @Override
          public void stop() {
            stopCount.incrementAndGet();
          }
        };

    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.initialize(configWith(false));
    context.registerPlugins();
    context.addPluginForTesting(pluginA);
    context.addPluginForTesting(pluginB);
    context.beforeExternalServices();
    context.startPlugins();

    assertThatCode(context::stopPlugins).doesNotThrowAnyException();
    // pluginB must still have been stopped despite pluginA throwing
    assertThat(stopCount.get()).isEqualTo(1);
  }

  /**
   * beforeExternalServices() throws (continueOnPluginError=false): plugin never reaches start() so
   * its watermark stays at REGISTERED. stopPlugins() must skip stop() for it.
   */
  @Test
  void stopPlugins_skipsStop_whenPluginFailedDuringBeforeExternalServices() {
    final AtomicInteger stopCount = new AtomicInteger(0);

    final BesuPlugin plugin =
        new NoOpPlugin() {
          @Override
          public void beforeExternalServices() {
            throw new RuntimeException("beforeExternalServices failure");
          }

          @Override
          public void stop() {
            stopCount.incrementAndGet();
          }
        };

    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.initialize(configWith(false));
    context.registerPlugins();
    context.addPluginForTesting(plugin);

    assertThatCode(context::beforeExternalServices).isInstanceOf(RuntimeException.class);
    assertThatCode(context::stopPlugins).doesNotThrowAnyException();
    assertThat(stopCount.get()).isEqualTo(0);
  }

  // -------------------------------------------------------------------------
  // Minimal no-op plugin base used by all stopPlugins() tests
  // -------------------------------------------------------------------------

  /** A do-nothing BesuPlugin whose lifecycle methods can be individually overridden. */
  private abstract static class NoOpPlugin implements BesuPlugin {

    @Override
    public void register(final ServiceManager context) {}

    @Override
    public void start() {}

    @Override
    public void stop() {}
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
