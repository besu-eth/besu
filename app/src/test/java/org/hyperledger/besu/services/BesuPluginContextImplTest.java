/*
 * Copyright contributors to Hyperledger Besu.
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

import org.hyperledger.besu.ethereum.api.pluginadapter.HealthCheckServiceImpl;
import org.hyperledger.besu.ethereum.core.plugins.ImmutablePluginConfiguration;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.RegistrationClosedException;
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.RegistrationService;
import org.hyperledger.besu.plugin.RunningContext;
import org.hyperledger.besu.plugin.RunningService;
import org.hyperledger.besu.plugin.StartContext;
import org.hyperledger.besu.plugin.StartService;
import org.hyperledger.besu.plugin.services.BesuService;
import org.hyperledger.besu.plugin.services.HealthCheckService;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;

public class BesuPluginContextImplTest {

  interface TestRegistrationService extends RegistrationService {}

  interface TestStartService extends StartService {}

  interface TestRunningService extends RunningService {}

  interface TestPluginService extends BesuService {}

  interface OtherPluginService extends BesuService {}

  /** A plugin whose phases run the given callbacks, under the given name. */
  static class RecordingPlugin implements BesuPlugin {
    private final String name;
    Consumer<RegistrationContext> onRegister = context -> {};
    Consumer<StartContext> onStart = context -> {};
    Consumer<RunningContext> onAfterMainLoop = context -> {};

    RecordingPlugin(final String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public void register(final RegistrationContext context) {
      onRegister.accept(context);
    }

    @Override
    public void start(final StartContext context) {
      onStart.accept(context);
    }

    @Override
    public void afterMainLoop(final RunningContext context) {
      onAfterMainLoop.accept(context);
    }

    @Override
    public void stop() {}
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
    public void register(final RegistrationContext context) {
      registerCalled = true;
      valueAtRegister = value;
    }

    @Override
    public void start(final StartContext context) {}

    @Override
    public void stop() {}
  }

  /** An unmigrated plugin that still adds its options from register(). */
  static class LateOptionsPlugin implements BesuPlugin {
    @Option(names = "--plugin-late-value")
    String value = "default";

    private PicoCLIOptions options;

    @Override
    public void defineOptions(final PicoCLIOptions options) {
      this.options = options;
    }

    @Override
    public void register(final RegistrationContext context) {
      options.addPicoCLIOptions("late", this);
    }

    @Override
    public void start(final StartContext context) {}

    @Override
    public void stop() {}
  }

  static class FailingRegisterPlugin implements BesuPlugin {
    @Override
    public void register(final RegistrationContext context) {
      throw new RuntimeException("cannot register");
    }

    @Override
    public void start(final StartContext context) {}

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
    public void register(final RegistrationContext context) {
      registerCalled = true;
    }

    @Override
    public void start(final StartContext context) {}

    @Override
    public void stop() {}
  }

  /** A context that loads the given plugins instead of scanning a plugins directory. */
  private static BesuPluginContextImpl contextLoading(final BesuPlugin... plugins) {
    return new BesuPluginContextImpl() {
      @Override
      List<BesuPlugin> loadPlugins() {
        return List.of(plugins);
      }
    };
  }

  /** Runs the define-options phase, through the real state transitions, for the given plugins. */
  private static BesuPluginContextImpl contextWithLoadedPlugins(
      final PicoCLIOptionsImpl picoCLIOptions,
      final boolean continueOnPluginError,
      final BesuPlugin... plugins) {
    final BesuPluginContextImpl context = contextLoading(plugins);
    context.initialize(
        ImmutablePluginConfiguration.builder()
            .continueOnPluginError(continueOnPluginError)
            .build());
    context.defineOptions(picoCLIOptions);
    return context;
  }

  /** A context whose plugins have defined their options, with the command line parsed. */
  private static BesuPluginContextImpl contextReadyToRegister(
      final boolean continueOnPluginError, final BesuPlugin... plugins) {
    final CommandLine commandLine = new CommandLine(CommandSpec.create());
    final PicoCLIOptionsImpl picoCLIOptions = new PicoCLIOptionsImpl(commandLine);
    final BesuPluginContextImpl context =
        contextWithLoadedPlugins(picoCLIOptions, continueOnPluginError, plugins);
    picoCLIOptions.optionsDefinitionCompleted();
    commandLine.parseArgs();
    return context;
  }

  private static void register(final BesuPluginContextImpl context) {
    context.beginRegistration();
    context.registerPlugins();
    context.endRegistration();
  }

  @Test
  void registrationContextServesRegistrationServices() {
    final TestRegistrationService service = new TestRegistrationService() {};
    final List<TestRegistrationService> seen = new ArrayList<>();
    final RecordingPlugin plugin = new RecordingPlugin("plugin");
    plugin.onRegister = context -> seen.add(context.getBesuService(TestRegistrationService.class));
    final BesuPluginContextImpl context = contextReadyToRegister(false, plugin);
    context.addService(TestRegistrationService.class, service);

    register(context);

    assertThat(seen).containsExactly(service);
  }

  @Test
  void registrationContextIsRejectedOnceRegistrationHasClosed() {
    final List<RegistrationContext> stashed = new ArrayList<>();
    final RecordingPlugin plugin = new RecordingPlugin("stasher");
    plugin.onRegister = stashed::add;
    final BesuPluginContextImpl context = contextReadyToRegister(false, plugin);
    context.addService(TestRegistrationService.class, new TestRegistrationService() {});
    register(context);

    assertThatThrownBy(() -> stashed.get(0).getBesuService(TestRegistrationService.class))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("stasher")
        .hasMessageContaining("register()");
    assertThatThrownBy(
            () ->
                stashed.get(0).registerService(TestPluginService.class, new TestPluginService() {}))
        .isInstanceOf(UnrecoverablePluginException.class)
        .hasMessageContaining("TestPluginService");
  }

  @Test
  void startAndRunningContextsServeTheServicesOfTheirTier() {
    final TestStartService startService = new TestStartService() {};
    final TestRunningService runningService = new TestRunningService() {};
    final List<Object> seen = new ArrayList<>();
    final RecordingPlugin plugin = new RecordingPlugin("plugin");
    plugin.onStart = context -> seen.add(context.getBesuService(TestStartService.class));
    plugin.onAfterMainLoop =
        context -> {
          // availability only widens: a start-tier service is still served once running
          seen.add(context.getBesuService(TestStartService.class));
          seen.add(context.getBesuService(TestRunningService.class));
        };
    final BesuPluginContextImpl context = contextReadyToRegister(false, plugin);
    register(context);

    context.addService(TestStartService.class, startService);
    context.startPlugins();
    context.addService(TestRunningService.class, runningService);
    context.afterMainLoop();

    assertThat(seen).containsExactly(startService, startService, runningService);
  }

  @Test
  void serviceNotProvidedOnThisNodeFailsThePluginNamingBoth() {
    final RecordingPlugin plugin = new RecordingPlugin("needy");
    plugin.onStart = context -> context.getBesuService(TestStartService.class);
    final BesuPluginContextImpl context = contextReadyToRegister(false, plugin);
    register(context);

    assertThatThrownBy(context::startPlugins)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(RecordingPlugin.class.getName())
        .cause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("needy")
        .hasMessageContaining("TestStartService")
        .hasMessageContaining("not provided on this node");
  }

  @Test
  void serviceNotProvidedOnThisNodeHonoursContinueOnError() {
    final RecordingPlugin needy = new RecordingPlugin("needy");
    needy.onStart = context -> context.getBesuService(TestStartService.class);
    final RecordingPlugin fine = new RecordingPlugin("fine");
    final BesuPluginContextImpl context = contextReadyToRegister(true, needy, fine);
    register(context);

    context.startPlugins();

    assertThat(context.getRegisteredPlugins()).containsExactly(fine);
  }

  @Test
  void pluginBuiltAgainstAnIncompatibleApiIsReportedAsSuch() {
    // what calling a lifecycle method on a plugin compiled against an older plugin API does
    final RecordingPlugin outdated = new RecordingPlugin("outdated");
    outdated.onRegister =
        context -> {
          throw new AbstractMethodError("register(RegistrationContext)");
        };
    final BesuPluginContextImpl context = contextReadyToRegister(false, outdated);
    context.beginRegistration();

    assertThatThrownBy(context::registerPlugins)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(RecordingPlugin.class.getName())
        .hasMessageContaining("not compatible with the plugin API of this Besu version")
        .hasCauseInstanceOf(AbstractMethodError.class);
  }

  @Test
  void pluginBuiltAgainstAnIncompatibleApiIsSkippedWhenContinueOnErrorIsSet() {
    final RecordingPlugin outdated = new RecordingPlugin("outdated");
    outdated.onRegister =
        context -> {
          throw new AbstractMethodError("register(RegistrationContext)");
        };
    final RecordingPlugin fine = new RecordingPlugin("fine");
    final BesuPluginContextImpl context = contextReadyToRegister(true, outdated, fine);

    register(context);
    context.startPlugins();

    assertThat(context.getRegisteredPlugins()).containsExactly(fine);
    assertThat(context.getPluginVersions()).containsKey("fine").doesNotContainKey("outdated");
  }

  @Test
  void pluginProvidedServiceIsVisibleToOtherPluginsAndToBesu() {
    final TestPluginService service = new TestPluginService() {};
    final RecordingPlugin provider = new RecordingPlugin("provider");
    provider.onRegister = context -> context.registerService(TestPluginService.class, service);
    final List<Object> seen = new ArrayList<>();
    final RecordingPlugin consumer = new RecordingPlugin("consumer");
    consumer.onStart =
        context -> {
          seen.add(context.getPluginService(TestPluginService.class).orElseThrow());
          seen.add(context.getPluginService(OtherPluginService.class).isPresent());
        };
    consumer.onAfterMainLoop =
        context -> seen.add(context.getPluginService(TestPluginService.class).orElseThrow());
    final BesuPluginContextImpl context = contextReadyToRegister(false, provider, consumer);

    register(context);
    context.startPlugins();
    context.afterMainLoop();

    assertThat(seen).containsExactly(service, false, service);
    assertThat(context.pluginProvidedServices().lookup(TestPluginService.class)).contains(service);
    assertThat(context.pluginProvidedServices().lookup(OtherPluginService.class)).isEmpty();
  }

  @Test
  void pluginProvidedServicesCannotBeReadWhileRegistrationIsOpen() {
    final BesuPluginContextImpl context = contextReadyToRegister(false);
    context.beginRegistration();

    assertThatThrownBy(context::pluginProvidedServices).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void registeringATypeTwiceFailsStartupNamingBothPlugins() {
    final RecordingPlugin first = new RecordingPlugin("first");
    first.onRegister =
        context -> context.registerService(TestPluginService.class, new TestPluginService() {});
    final RecordingPlugin second = new RecordingPlugin("second");
    second.onRegister =
        context -> context.registerService(TestPluginService.class, new TestPluginService() {});
    // fatal even when the operator chose to continue on plugin errors
    final BesuPluginContextImpl context = contextReadyToRegister(true, first, second);
    context.beginRegistration();

    assertThatThrownBy(context::registerPlugins)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(RecordingPlugin.class.getName())
        .hasMessageContaining("TestPluginService")
        .hasMessageContaining("first")
        .hasCauseInstanceOf(UnrecoverablePluginException.class);
  }

  @Test
  void registeringABesuProvidedTypeFailsStartup() {
    final RecordingPlugin plugin = new RecordingPlugin("impostor");
    plugin.onRegister =
        context -> context.registerService(TestStartService.class, new TestStartService() {});
    final BesuPluginContextImpl context = contextReadyToRegister(true, plugin);
    context.beginRegistration();

    assertThatThrownBy(context::registerPlugins)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("TestStartService")
        .hasMessageContaining("Besu-provided")
        .hasCauseInstanceOf(UnrecoverablePluginException.class);
  }

  @Test
  void pluginServiceLookupRejectsBesuProvidedTypes() {
    final RecordingPlugin plugin = new RecordingPlugin("plugin");
    final BesuPluginContextImpl context = contextReadyToRegister(false, plugin);
    context.addService(TestRegistrationService.class, new TestRegistrationService() {});
    register(context);

    assertThatThrownBy(
            () -> context.startContextFor(plugin).getPluginService(TestRegistrationService.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("TestRegistrationService")
        .hasMessageContaining("Besu-provided");
  }

  @Test
  void registrationThroughAStashedServiceAfterRegisterIsFatal() {
    final List<HealthCheckService> stashed = new ArrayList<>();
    final RecordingPlugin plugin = new RecordingPlugin("late");
    plugin.onRegister = context -> stashed.add(context.getBesuService(HealthCheckService.class));
    plugin.onStart =
        context ->
            stashed
                .get(0)
                .registerHealthCheck(
                    "/late", params -> HealthCheckService.HealthCheckResult.of(true));
    // fatal even when the operator chose to continue on plugin errors
    final BesuPluginContextImpl context = contextReadyToRegister(true, plugin);
    final HealthCheckServiceImpl healthCheckService =
        new HealthCheckServiceImpl(context::isRegistering);
    context.addService(HealthCheckService.class, healthCheckService);
    register(context);

    assertThatThrownBy(context::startPlugins)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(RecordingPlugin.class.getName())
        .hasMessageContaining("registerHealthCheck")
        .hasCauseInstanceOf(RegistrationClosedException.class);
    assertThat(healthCheckService.getHealthCheck("/late")).isEmpty();
  }

  @Test
  void registrationThroughTheServiceWorksWhileRegistrationIsOpen() {
    final RecordingPlugin plugin = new RecordingPlugin("on-time");
    plugin.onRegister =
        context ->
            context
                .getBesuService(HealthCheckService.class)
                .registerHealthCheck(
                    "/on-time", params -> HealthCheckService.HealthCheckResult.of(true));
    final BesuPluginContextImpl context = contextReadyToRegister(false, plugin);
    final HealthCheckServiceImpl healthCheckService =
        new HealthCheckServiceImpl(context::isRegistering);
    context.addService(HealthCheckService.class, healthCheckService);

    register(context);

    assertThat(healthCheckService.getHealthCheck("/on-time")).isPresent();
  }

  @Test
  void serviceRegistryHandlesConcurrentReadsAndWrites() throws Exception {
    final RecordingPlugin plugin = new RecordingPlugin("plugin");
    final BesuPluginContextImpl context = contextReadyToRegister(false, plugin);
    register(context);
    final StartContext startContext = context.startContextFor(plugin);
    final int threadCount = 10;
    final int operationsPerThread = 100;
    final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final AtomicBoolean failed = new AtomicBoolean(false);
    final List<Future<?>> futures = new ArrayList<>();

    // Pre-register one service so readers have something to find
    final TestStartService service = new TestStartService() {};
    context.addService(TestStartService.class, service);

    // Half the threads add services, half read services concurrently
    for (int i = 0; i < threadCount; i++) {
      final int threadIndex = i;
      futures.add(
          executor.submit(
              () -> {
                try {
                  startLatch.await();
                  for (int op = 0; op < operationsPerThread; op++) {
                    if (threadIndex % 2 == 0) {
                      context.addService(TestRunningService.class, new TestRunningService() {});
                    } else {
                      startContext.getBesuService(TestStartService.class);
                      startContext.getPluginService(TestPluginService.class);
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
    assertThat(startContext.getBesuService(TestStartService.class)).isSameAs(service);
    assertThat(context.runningContextFor(plugin).getBesuService(TestRunningService.class))
        .isNotNull();
  }

  @Test
  void registerSeesOptionValuesBoundByTheParse() {
    final CommandLine commandLine = new CommandLine(CommandSpec.create());
    final PicoCLIOptionsImpl picoCLIOptions = new PicoCLIOptionsImpl(commandLine);
    final OptionPlugin plugin = new OptionPlugin();
    final BesuPluginContextImpl context = contextWithLoadedPlugins(picoCLIOptions, false, plugin);

    picoCLIOptions.optionsDefinitionCompleted();
    commandLine.parseArgs("--plugin-test-value=configured");
    register(context);

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
    context.beginRegistration();

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
    register(context);

    assertThat(failing.registerCalled).isFalse();
    assertThat(good.registerCalled).isTrue();
    assertThat(context.getRegisteredPlugins()).containsExactly(good);
  }

  @Test
  void pluginFailingInRegisterIsNotListedWhenContinueOnErrorIsSet() {
    final CommandLine commandLine = new CommandLine(CommandSpec.create());
    final PicoCLIOptionsImpl picoCLIOptions = new PicoCLIOptionsImpl(commandLine);
    final FailingRegisterPlugin failing = new FailingRegisterPlugin();
    final OptionPlugin good = new OptionPlugin();
    final BesuPluginContextImpl context =
        contextWithLoadedPlugins(picoCLIOptions, true, failing, good);
    assertThat(context.getPluginVersions()).containsKey(failing.getName());

    picoCLIOptions.optionsDefinitionCompleted();
    commandLine.parseArgs();
    register(context);

    assertThat(context.getRegisteredPlugins()).containsExactly(good);
    assertThat(context.getPluginVersions())
        .containsKey(good.getName())
        .doesNotContainKey(failing.getName());
  }

  @Test
  void pluginFailingInDefineOptionsFailsStartupByDefault() {
    final CommandLine commandLine = new CommandLine(CommandSpec.create());
    final PicoCLIOptionsImpl picoCLIOptions = new PicoCLIOptionsImpl(commandLine);
    final BesuPluginContextImpl context = contextLoading(new FailingDefineOptionsPlugin());
    context.initialize(ImmutablePluginConfiguration.builder().build());

    assertThatThrownBy(() -> context.defineOptions(picoCLIOptions))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(FailingDefineOptionsPlugin.class.getName());
  }

  @Test
  void registrationRequiresDefineOptionsFirst() {
    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.initialize(
        ImmutablePluginConfiguration.builder().externalPluginsEnabled(false).build());

    assertThatThrownBy(context::beginRegistration).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void registerPluginsRequiresRegistrationToBeOpen() {
    final BesuPluginContextImpl context = contextReadyToRegister(false);

    assertThatThrownBy(context::registerPlugins).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(context::endRegistration).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void resetStateAllowsRegisteringTheLoadedPluginsAgain() {
    final CommandLine commandLine = new CommandLine(CommandSpec.create());
    final PicoCLIOptionsImpl picoCLIOptions = new PicoCLIOptionsImpl(commandLine);
    final OptionPlugin plugin = new OptionPlugin();
    final BesuPluginContextImpl context = contextWithLoadedPlugins(picoCLIOptions, false, plugin);
    picoCLIOptions.optionsDefinitionCompleted();
    commandLine.parseArgs("--plugin-test-value=configured");
    register(context);

    context.resetState();
    plugin.valueAtRegister = null;
    register(context);

    assertThat(plugin.valueAtRegister).isEqualTo("configured");
    assertThat(context.getRegisteredPlugins()).containsExactly(plugin);
  }

  @Test
  void resetStateLetsAPluginPublishItsServiceAgain() {
    final RecordingPlugin provider = new RecordingPlugin("provider");
    provider.onRegister =
        context -> context.registerService(TestPluginService.class, new TestPluginService() {});
    final BesuPluginContextImpl context = contextReadyToRegister(false, provider);
    register(context);
    final TestPluginService firstCycle =
        context.pluginProvidedServices().lookup(TestPluginService.class).orElseThrow();

    // the Ephemery restart registers the same plugin instances again
    context.resetState();
    assertThat(context.pluginProvidedServices().lookup(TestPluginService.class)).isEmpty();
    register(context);

    assertThat(context.pluginProvidedServices().lookup(TestPluginService.class))
        .isPresent()
        .get()
        .isNotSameAs(firstCycle);
  }
}
