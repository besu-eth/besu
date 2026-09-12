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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.core.plugins.ImmutablePluginConfiguration;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.BesuService;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;

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
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;

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

  /** A plugin that declares one option and records the value it sees in each phase. */
  static class OptionPlugin implements BesuPlugin {
    @Option(names = "--plugin-test-value")
    String value = "default";

    String valueAtDefineOptions;
    String valueAtRegister;
    boolean registerCalled;

    @Override
    public void defineOptions(final PicoCLIOptions options) {
      valueAtDefineOptions = value;
      options.addPicoCLIOptions("test", this);
    }

    @Override
    public void register(final ServiceManager context) {
      registerCalled = true;
      valueAtRegister = value;
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}
  }

  /** An unmigrated plugin that still adds its options from register(). */
  static class LateOptionsPlugin implements BesuPlugin {
    @Option(names = "--plugin-late-value")
    String value = "default";

    @Override
    public void register(final ServiceManager context) {
      context.getService(PicoCLIOptions.class).orElseThrow().addPicoCLIOptions("late", this);
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}
  }

  static class FailingDefineOptionsPlugin implements BesuPlugin {
    boolean registerCalled;

    @Override
    public void defineOptions(final PicoCLIOptions options) {
      throw new RuntimeException("cannot define options");
    }

    @Override
    public void register(final ServiceManager context) {
      registerCalled = true;
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}
  }

  /** Runs the define-options phase for the given plugins without scanning a plugins directory. */
  private static BesuPluginContextImpl contextWithLoadedPlugins(
      final PicoCLIOptionsImpl picoCLIOptions,
      final boolean continueOnPluginError,
      final BesuPlugin... plugins) {
    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.addService(PicoCLIOptions.class, picoCLIOptions);
    context.initialize(
        ImmutablePluginConfiguration.builder()
            .externalPluginsEnabled(false)
            .continueOnPluginError(continueOnPluginError)
            .build());
    context.defineOptions(picoCLIOptions);
    context.defineOptions(picoCLIOptions, List.of(plugins));
    return context;
  }

  @Test
  void registerSeesOptionValuesBoundByTheParse() {
    final CommandLine commandLine = new CommandLine(CommandSpec.create());
    final PicoCLIOptionsImpl picoCLIOptions = new PicoCLIOptionsImpl(commandLine);
    final OptionPlugin plugin = new OptionPlugin();
    final BesuPluginContextImpl context = contextWithLoadedPlugins(picoCLIOptions, false, plugin);

    picoCLIOptions.optionsDefinitionCompleted();
    commandLine.parseArgs("--plugin-test-value=configured");
    context.registerPlugins();

    assertThat(plugin.valueAtDefineOptions).isEqualTo("default");
    assertThat(plugin.valueAtRegister).isEqualTo("configured");
    assertThat(context.getPluginVersions()).containsKey(plugin.getName());
    assertThat(context.getRegisteredPlugins()).containsExactly(plugin);
  }

  @Test
  void addingOptionsInRegisterFailsStartupEvenWhenContinueOnErrorIsSet() {
    final CommandLine commandLine = new CommandLine(CommandSpec.create());
    final PicoCLIOptionsImpl picoCLIOptions = new PicoCLIOptionsImpl(commandLine);
    final BesuPluginContextImpl context =
        contextWithLoadedPlugins(picoCLIOptions, true, new LateOptionsPlugin());

    picoCLIOptions.optionsDefinitionCompleted();
    commandLine.parseArgs();

    assertThatThrownBy(context::registerPlugins)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(LateOptionsPlugin.class.getName())
        .hasMessageContaining("defineOptions()")
        .hasCauseInstanceOf(PicoCLIOptionsImpl.OptionsAlreadyParsedException.class);
  }

  @Test
  void pluginFailingInDefineOptionsIsNotRegisteredWhenContinueOnErrorIsSet() {
    final CommandLine commandLine = new CommandLine(CommandSpec.create());
    final PicoCLIOptionsImpl picoCLIOptions = new PicoCLIOptionsImpl(commandLine);
    final FailingDefineOptionsPlugin failing = new FailingDefineOptionsPlugin();
    final OptionPlugin good = new OptionPlugin();
    final BesuPluginContextImpl context =
        contextWithLoadedPlugins(picoCLIOptions, true, failing, good);

    picoCLIOptions.optionsDefinitionCompleted();
    commandLine.parseArgs();
    context.registerPlugins();

    assertThat(failing.registerCalled).isFalse();
    assertThat(good.registerCalled).isTrue();
    assertThat(context.getRegisteredPlugins()).containsExactly(good);
  }

  @Test
  void pluginFailingInDefineOptionsFailsStartupByDefault() {
    final CommandLine commandLine = new CommandLine(CommandSpec.create());
    final PicoCLIOptionsImpl picoCLIOptions = new PicoCLIOptionsImpl(commandLine);
    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.initialize(
        ImmutablePluginConfiguration.builder().externalPluginsEnabled(false).build());
    context.defineOptions(picoCLIOptions);

    assertThatThrownBy(
            () -> context.defineOptions(picoCLIOptions, List.of(new FailingDefineOptionsPlugin())))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(FailingDefineOptionsPlugin.class.getName());
  }

  @Test
  void registerPluginsRequiresDefineOptionsFirst() {
    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.initialize(
        ImmutablePluginConfiguration.builder().externalPluginsEnabled(false).build());

    assertThatThrownBy(context::registerPlugins).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void resetStateAllowsRegisteringTheLoadedPluginsAgain() {
    final CommandLine commandLine = new CommandLine(CommandSpec.create());
    final PicoCLIOptionsImpl picoCLIOptions = new PicoCLIOptionsImpl(commandLine);
    final OptionPlugin plugin = new OptionPlugin();
    final BesuPluginContextImpl context = contextWithLoadedPlugins(picoCLIOptions, false, plugin);
    picoCLIOptions.optionsDefinitionCompleted();
    commandLine.parseArgs("--plugin-test-value=configured");
    context.registerPlugins();

    context.resetState();
    plugin.valueAtRegister = null;
    context.registerPlugins();

    assertThat(plugin.valueAtRegister).isEqualTo("configured");
    assertThat(context.getRegisteredPlugins()).containsExactly(plugin);
  }
}
