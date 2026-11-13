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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.hyperledger.besu.ethereum.api.pluginadapter.HealthCheckServiceImpl;
import org.hyperledger.besu.ethereum.core.plugins.ImmutablePluginConfiguration;
import org.hyperledger.besu.ethereum.core.plugins.PluginInfo;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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
    Runnable onStop = () -> {};

    RecordingPlugin(final String name) {
      this.name = name;
    }

    @Override
    public String toString() {
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
    public void stop() {
      onStop.run();
    }
  }

  /** A recording plugin of its own class, so it has an id distinct from {@link RecordingPlugin}. */
  static class BuiltInRecordingPlugin extends RecordingPlugin {
    BuiltInRecordingPlugin(final String name) {
      super(name);
    }
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

  /** A context that detects the given plugins instead of scanning a plugins directory. */
  private static BesuPluginContextImpl contextLoading(final BesuPlugin... plugins) {
    return contextDetecting(Stream.of(plugins).map(BesuPluginContextImplTest::providerOf).toList());
  }

  /** A context that detects the given providers instead of scanning a plugins directory. */
  private static BesuPluginContextImpl contextDetecting(
      final List<ServiceLoader.Provider<BesuPlugin>> providers) {
    return contextDetecting(providers, List.of());
  }

  /**
   * A context that detects the given providers, and the given plugins whose class could not be
   * loaded, instead of scanning a plugins directory.
   */
  private static BesuPluginContextImpl contextDetecting(
      final List<ServiceLoader.Provider<BesuPlugin>> providers,
      final List<BesuPluginContextImpl.UnloadablePlugin> unloadable) {
    return new BesuPluginContextImpl() {
      @Override
      DetectedPlugins detectPlugins() {
        return new DetectedPlugins(providers, unloadable);
      }
    };
  }

  /** A plugin whose parent class comes from a library that was not copied next to its jar. */
  private static BesuPluginContextImpl.UnloadablePlugin pluginMissingItsLibrary() {
    return new BesuPluginContextImpl.UnloadablePlugin(
        "com.acme.BridgePlugin",
        Path.of("plugins", "bridge.jar"),
        new NoClassDefFoundError("com/acme/LibBase"));
  }

  /** What the service loader hands over for an already constructed plugin. */
  private static ServiceLoader.Provider<BesuPlugin> providerOf(final BesuPlugin plugin) {
    return new ServiceLoader.Provider<>() {
      @Override
      public Class<? extends BesuPlugin> type() {
        return plugin.getClass();
      }

      @Override
      public BesuPlugin get() {
        return plugin;
      }
    };
  }

  /** What the service loader hands over for a plugin whose constructor throws. */
  private static ServiceLoader.Provider<BesuPlugin> failingProviderOf(
      final Class<? extends BesuPlugin> pluginType, final Throwable cause) {
    return new ServiceLoader.Provider<>() {
      @Override
      public Class<? extends BesuPlugin> type() {
        return pluginType;
      }

      @Override
      public BesuPlugin get() {
        throw new ServiceConfigurationError(
            "Provider " + pluginType.getName() + " could not be instantiated", cause);
      }
    };
  }

  /**
   * Runs the define-options phase, through the real selection and instantiation, for the given
   * detected plugins and {@code --plugins} list, with the command line parsed.
   */
  private static BesuPluginContextImpl contextSelecting(
      final boolean continueOnPluginError,
      final List<String> requestedPlugins,
      final List<ServiceLoader.Provider<BesuPlugin>> detected) {
    return contextSelecting(continueOnPluginError, requestedPlugins, detected, List.of());
  }

  /** As above, with detected plugins whose class could not be loaded. */
  private static BesuPluginContextImpl contextSelecting(
      final boolean continueOnPluginError,
      final List<String> requestedPlugins,
      final List<ServiceLoader.Provider<BesuPlugin>> detected,
      final List<BesuPluginContextImpl.UnloadablePlugin> unloadable) {
    final CommandLine commandLine = new CommandLine(CommandSpec.create());
    final PicoCLIOptionsImpl picoCLIOptions = new PicoCLIOptionsImpl(commandLine);
    final BesuPluginContextImpl context = contextDetecting(detected, unloadable);
    context.initialize(
        ImmutablePluginConfiguration.builder()
            .continueOnPluginError(continueOnPluginError)
            .requestedPluginsInfo(requestedPlugins.stream().map(PluginInfo::new).toList())
            .build());
    context.defineOptions(picoCLIOptions);
    picoCLIOptions.optionsDefinitionCompleted();
    commandLine.parseArgs();
    return context;
  }

  /** A context that scans the given plugins directory, with its options phase not yet run. */
  private static BesuPluginContextImpl contextScanning(final Path pluginsDir) {
    return contextScanning(pluginsDir, List.of());
  }

  /** As above, with the given {@code --plugins} list. */
  private static BesuPluginContextImpl contextScanning(
      final Path pluginsDir, final List<String> requestedPlugins) {
    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.initialize(
        ImmutablePluginConfiguration.builder()
            .pluginsDir(pluginsDir)
            .requestedPluginsInfo(requestedPlugins.stream().map(PluginInfo::new).toList())
            .build());
    return context;
  }

  private static PicoCLIOptionsImpl freshPicoCLIOptions() {
    return new PicoCLIOptionsImpl(new CommandLine(CommandSpec.create()));
  }

  /** Writes a jar holding only a service file that declares the given plugin classes. */
  private static void writeJarDeclaring(final Path jar, final String... pluginClassNames)
      throws IOException {
    try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar))) {
      out.putNextEntry(new ZipEntry("META-INF/services/" + BesuPlugin.class.getName()));
      out.write(String.join("\n", pluginClassNames).getBytes(StandardCharsets.UTF_8));
      out.closeEntry();
    }
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

  /**
   * A context with the given built-in plugins, loading the given external plugins, with the options
   * defined and the command line parsed.
   */
  private static BesuPluginContextImpl contextWithBuiltIns(
      final boolean continueOnPluginError,
      final List<BesuPlugin> builtIns,
      final BesuPlugin... externalPlugins) {
    final CommandLine commandLine = new CommandLine(CommandSpec.create());
    final PicoCLIOptionsImpl picoCLIOptions = new PicoCLIOptionsImpl(commandLine);
    final BesuPluginContextImpl context = contextLoading(externalPlugins);
    builtIns.forEach(context::addBuiltInPlugin);
    context.initialize(
        ImmutablePluginConfiguration.builder()
            .continueOnPluginError(continueOnPluginError)
            .build());
    context.defineOptions(picoCLIOptions);
    picoCLIOptions.optionsDefinitionCompleted();
    commandLine.parseArgs();
    return context;
  }

  @Test
  void builtInPluginsRunEveryPhaseAheadOfTheExternalPlugins() {
    final List<String> order = new ArrayList<>();
    final RecordingPlugin builtIn = new RecordingPlugin("builtIn");
    builtIn.onRegister = context -> order.add("builtIn.register");
    builtIn.onStart = context -> order.add("builtIn.start");
    builtIn.onAfterMainLoop = context -> order.add("builtIn.afterMainLoop");
    builtIn.onStop = () -> order.add("builtIn.stop");
    final RecordingPlugin external = new RecordingPlugin("external");
    external.onRegister = context -> order.add("external.register");
    external.onStart = context -> order.add("external.start");
    external.onAfterMainLoop = context -> order.add("external.afterMainLoop");
    external.onStop = () -> order.add("external.stop");
    final BesuPluginContextImpl context = contextWithBuiltIns(false, List.of(builtIn), external);

    register(context);
    context.startPlugins();
    context.afterMainLoop();
    context.stopPlugins();

    assertThat(order)
        .containsExactly(
            "builtIn.register",
            "external.register",
            "builtIn.start",
            "external.start",
            "builtIn.afterMainLoop",
            "external.afterMainLoop",
            "builtIn.stop",
            "external.stop");
  }

  @Test
  void builtInPluginsRunWhenExternalPluginsAreDisabled() {
    final RecordingPlugin builtIn = new RecordingPlugin("builtIn");
    final AtomicBoolean registered = new AtomicBoolean();
    builtIn.onRegister = context -> registered.set(true);
    final CommandLine commandLine = new CommandLine(CommandSpec.create());
    final PicoCLIOptionsImpl picoCLIOptions = new PicoCLIOptionsImpl(commandLine);
    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.addBuiltInPlugin(builtIn);
    context.initialize(
        ImmutablePluginConfiguration.builder().externalPluginsEnabled(false).build());
    context.defineOptions(picoCLIOptions);
    picoCLIOptions.optionsDefinitionCompleted();
    commandLine.parseArgs();

    register(context);

    assertThat(registered).isTrue();
    assertThat(context.getRegisteredPlugins()).containsExactly(builtIn);
  }

  @Test
  void builtInPluginFailingToDefineOptionsIsFatalEvenWithContinueOnError() {
    final CommandLine commandLine = new CommandLine(CommandSpec.create());
    final PicoCLIOptionsImpl picoCLIOptions = new PicoCLIOptionsImpl(commandLine);
    final BesuPluginContextImpl context = contextLoading();
    context.addBuiltInPlugin(new FailingDefineOptionsPlugin());
    context.initialize(ImmutablePluginConfiguration.builder().continueOnPluginError(true).build());

    assertThatThrownBy(() -> context.defineOptions(picoCLIOptions))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Error defining options of plugin of type")
        .hasMessageContaining(
            "a built-in plugin failure is fatal regardless of --plugin-continue-on-error");
  }

  @Test
  void builtInPluginFailingToRegisterIsFatalEvenWithContinueOnError() {
    final BesuPluginContextImpl context =
        contextWithBuiltIns(true, List.of(new FailingRegisterPlugin()));
    context.beginRegistration();

    assertThatThrownBy(context::registerPlugins)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Error registering plugin of type")
        .hasMessageContaining(
            "a built-in plugin failure is fatal regardless of --plugin-continue-on-error");
  }

  @Test
  void builtInPluginFailingToStartIsFatalEvenWithContinueOnError() {
    final RecordingPlugin builtIn = new RecordingPlugin("builtIn");
    builtIn.onStart =
        context -> {
          throw new RuntimeException("cannot start");
        };
    final BesuPluginContextImpl context = contextWithBuiltIns(true, List.of(builtIn));
    register(context);

    assertThatThrownBy(context::startPlugins)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Error starting plugin of type")
        .hasMessageContaining("cannot start");
  }

  @Test
  void builtInPluginFailingAfterTheMainLoopIsFatalEvenWithContinueOnError() {
    final RecordingPlugin builtIn = new RecordingPlugin("builtIn");
    builtIn.onAfterMainLoop =
        context -> {
          throw new RuntimeException("cannot run");
        };
    final BesuPluginContextImpl context = contextWithBuiltIns(true, List.of(builtIn));
    register(context);
    context.startPlugins();

    assertThatThrownBy(context::afterMainLoop)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Error calling `afterMainLoop` on plugin of type")
        .hasMessageContaining("cannot run");
  }

  @Test
  void externalPluginFailureIsStillSkippedWithContinueOnError() {
    final RecordingPlugin builtIn = new RecordingPlugin("builtIn");
    final BesuPluginContextImpl context =
        contextWithBuiltIns(true, List.of(builtIn), new FailingRegisterPlugin());

    register(context);

    assertThat(context.getRegisteredPlugins()).containsExactly(builtIn);
  }

  @Test
  void builtInPluginsAreListedWithTheExternalOnes() {
    final RecordingPlugin builtIn = new BuiltInRecordingPlugin("builtIn");
    final RecordingPlugin external = new RecordingPlugin("external");
    final BesuPluginContextImpl context = contextWithBuiltIns(false, List.of(builtIn), external);

    register(context);

    assertThat(context.getPluginVersions())
        .containsKeys(BuiltInRecordingPlugin.class.getName(), RecordingPlugin.class.getName());
    assertThat(context.getPluginsById())
        .containsKeys(BuiltInRecordingPlugin.class.getName(), RecordingPlugin.class.getName());
  }

  @Test
  void summaryListsTheBuiltInsApartAndCountsOnlyTheExternalPlugins() {
    final RecordingPlugin builtIn = new BuiltInRecordingPlugin("builtIn");
    final RecordingPlugin external = new RecordingPlugin("external");
    final BesuPluginContextImpl context = contextWithBuiltIns(false, List.of(builtIn), external);
    register(context);

    final List<String> summary = context.getPluginsSummaryLog();

    assertThat(summary)
        .containsSubsequence(
            "Plugin Registration Summary:",
            "Built-in plugins:",
            " - " + BuiltInRecordingPlugin.class.getName() + " (" + builtIn.getVersion() + ")",
            "Registered Plugins:",
            " - " + RecordingPlugin.class.getName() + " (" + external.getVersion() + ")")
        .doesNotContain("No external plugins have been registered.");
    // the total counts the plugins directory, which the built-in is not part of: one external
    // plugin detected, one registered
    assertThat(summary.getLast()).isEqualTo("TOTAL = 1 of 1 plugins successfully registered.");
  }

  @Test
  void summaryStillReportsTheBuiltInsWhenNoExternalPluginRegistered() {
    final BesuPluginContextImpl context =
        contextWithBuiltIns(false, List.of(new RecordingPlugin("builtIn")));
    register(context);

    assertThat(context.getPluginsSummaryLog())
        .containsSubsequence("Built-in plugins:", "No external plugins have been registered.")
        .doesNotContain("Registered Plugins:");
  }

  @Test
  void configurationOverviewLinesAreCollectedByPluginNameLeavingOutSilentPlugins() {
    final RecordingPlugin talkative =
        new RecordingPlugin("talkative") {
          @Override
          public List<String> configurationOverviewLines() {
            return List.of("one", "two");
          }
        };
    final RecordingPlugin silent = new RecordingPlugin("silent");
    final BesuPluginContextImpl context = contextWithBuiltIns(false, List.of(talkative), silent);
    register(context);

    assertThat(context.getConfigurationOverviewLines())
        .containsOnlyKeys(talkative.getClass().getName())
        .containsEntry(talkative.getClass().getName(), List.of("one", "two"));
  }

  /** A plugin whose overview lines cannot be read. */
  private static RecordingPlugin pluginFailingTheOverview(final String name) {
    return new RecordingPlugin(name) {
      @Override
      public List<String> configurationOverviewLines() {
        throw new RuntimeException("no overview");
      }
    };
  }

  @Test
  void externalPluginFailingTheOverviewIsLeftOutWithContinueOnError() {
    final RecordingPlugin fine =
        new RecordingPlugin("fine") {
          @Override
          public List<String> configurationOverviewLines() {
            return List.of("one");
          }
        };
    final BesuPluginContextImpl context =
        contextWithBuiltIns(true, List.of(), pluginFailingTheOverview("broken"), fine);
    register(context);

    assertThat(context.getConfigurationOverviewLines())
        .containsOnlyKeys(fine.getClass().getName())
        .containsEntry(fine.getClass().getName(), List.of("one"));
  }

  @Test
  void externalPluginFailingTheOverviewIsFatalWithoutContinueOnError() {
    final BesuPluginContextImpl context =
        contextWithBuiltIns(false, List.of(), pluginFailingTheOverview("broken"));
    register(context);

    assertThatThrownBy(context::getConfigurationOverviewLines)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Error reading the configuration overview of plugin of type")
        .hasMessageContaining("no overview");
  }

  @Test
  void builtInPluginFailingTheOverviewIsFatalEvenWithContinueOnError() {
    final BesuPluginContextImpl context =
        contextWithBuiltIns(true, List.of(pluginFailingTheOverview("broken")));
    register(context);

    assertThatThrownBy(context::getConfigurationOverviewLines)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Error reading the configuration overview of plugin of type")
        .hasMessageContaining(
            "a built-in plugin failure is fatal regardless of --plugin-continue-on-error");
  }

  @Test
  void restartStopsThePluginsAndRegistersTheSameInstancesAgain() {
    final List<String> events = new ArrayList<>();
    final RecordingPlugin builtIn = new RecordingPlugin("builtIn");
    builtIn.onRegister = context -> events.add("builtIn.register");
    builtIn.onStop = () -> events.add("builtIn.stop");
    final RecordingPlugin external = new RecordingPlugin("external");
    external.onRegister = context -> events.add("external.register");
    external.onStop = () -> events.add("external.stop");
    final BesuPluginContextImpl context = contextWithBuiltIns(false, List.of(builtIn), external);
    register(context);
    context.startPlugins();
    context.afterMainLoop();
    events.clear();

    context.stopPluginsForRestart();
    context.resetState();
    register(context);
    context.startPlugins();

    assertThat(events)
        .containsExactly("builtIn.stop", "external.stop", "builtIn.register", "external.register");
    assertThat(context.getRegisteredPlugins()).containsExactly(builtIn, external);
  }

  @Test
  void restartStopsNothingWhenThePluginsNeverStarted() {
    final List<String> events = new ArrayList<>();
    final RecordingPlugin builtIn = new RecordingPlugin("builtIn");
    builtIn.onStop = () -> events.add("builtIn.stop");
    final BesuPluginContextImpl context = contextWithBuiltIns(false, List.of(builtIn));
    register(context);

    context.stopPluginsForRestart();
    context.resetState();
    register(context);

    assertThat(events).isEmpty();
    assertThat(context.getRegisteredPlugins()).containsExactly(builtIn);
  }

  @Test
  void restartAfterAFailedStartStopsOnlyThePluginsThatStartedAndRunsTheNextCycle() {
    final List<String> events = new ArrayList<>();
    final RecordingPlugin started = new RecordingPlugin("started");
    started.onStart = context -> events.add("started.start");
    started.onStop = () -> events.add("started.stop");
    final BuiltInRecordingPlugin failing = new BuiltInRecordingPlugin("failing");
    failing.onStart =
        context -> {
          throw new RuntimeException("start failed");
        };
    failing.onStop = () -> events.add("failing.stop");
    final RecordingPlugin external = new RecordingPlugin("external");
    external.onStart = context -> events.add("external.start");
    external.onStop = () -> events.add("external.stop");
    final BesuPluginContextImpl context =
        contextWithBuiltIns(false, List.of(started, failing), external);
    register(context);
    assertThatThrownBy(context::startPlugins).hasMessageContaining("start failed");
    events.clear();

    // the next cycle: only the plugin whose start() returned is stopped, then the cycle proceeds
    context.stopPluginsForRestart();
    assertThat(events).containsExactly("started.stop");
    failing.onStart = startContext -> events.add("failing.start");
    context.resetState();
    register(context);
    context.startPlugins();

    assertThat(events)
        .containsExactly("started.stop", "started.start", "failing.start", "external.start");
  }

  @Test
  void restartDoesNotStopAPluginDroppedAfterTheMainLoop() {
    final List<String> events = new ArrayList<>();
    final RecordingPlugin dropped = new RecordingPlugin("dropped");
    dropped.onAfterMainLoop =
        context -> {
          throw new RuntimeException("afterMainLoop failed");
        };
    dropped.onStop = () -> events.add("dropped.stop");
    final BesuPluginContextImpl context = contextWithBuiltIns(true, List.of(), dropped);
    register(context);
    context.startPlugins();
    context.afterMainLoop();

    context.stopPluginsForRestart();

    assertThat(events).isEmpty();
  }

  @Test
  void restartIsRejectedOnceThePluginsAreStopped() {
    final BesuPluginContextImpl context =
        contextWithBuiltIns(false, List.of(new RecordingPlugin("builtIn")));
    register(context);
    context.startPlugins();
    context.afterMainLoop();
    context.stopPlugins();

    assertThatThrownBy(context::stopPluginsForRestart)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("STOPPED");
  }

  @Test
  void resetStateIsRejectedWhileThePluginsRun() {
    final BesuPluginContextImpl context =
        contextWithBuiltIns(false, List.of(new RecordingPlugin("builtIn")));
    register(context);
    context.startPlugins();

    assertThatThrownBy(context::resetState)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("stop the plugins of the previous cycle first");
  }

  @Test
  void resetStateIsRejectedAfterAFailedStartUntilThePluginsAreStopped() {
    final BuiltInRecordingPlugin failing = new BuiltInRecordingPlugin("failing");
    failing.onStart =
        context -> {
          throw new RuntimeException("start failed");
        };
    final BesuPluginContextImpl context =
        contextWithBuiltIns(false, List.of(new RecordingPlugin("started"), failing));
    register(context);
    assertThatThrownBy(context::startPlugins).hasMessageContaining("start failed");

    assertThatThrownBy(context::resetState)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("BEFORE_MAIN_LOOP_STARTED");

    context.stopPluginsForRestart();
    context.resetState();
    register(context);
    assertThat(context.getRegisteredPlugins()).hasSize(2);
  }

  @Test
  void restartAfterAFatalAfterMainLoopFailureStopsEveryStartedPlugin() {
    final List<String> events = new ArrayList<>();
    final RecordingPlugin builtIn = new RecordingPlugin("builtIn");
    builtIn.onStop = () -> events.add("builtIn.stop");
    final RecordingPlugin failing = new RecordingPlugin("failing");
    failing.onAfterMainLoop =
        context -> {
          throw new RuntimeException("afterMainLoop failed");
        };
    failing.onStop = () -> events.add("failing.stop");
    final BesuPluginContextImpl context = contextWithBuiltIns(false, List.of(builtIn), failing);
    register(context);
    context.startPlugins();
    assertThatThrownBy(context::afterMainLoop).hasMessageContaining("afterMainLoop failed");

    // both started, so both are stopped: the failure came after their start() returned
    context.stopPluginsForRestart();

    assertThat(events).containsExactly("builtIn.stop", "failing.stop");
  }

  @Test
  void restartDoesNotStopAPluginDroppedWhileStarting() {
    final List<String> events = new ArrayList<>();
    final RecordingPlugin builtIn = new RecordingPlugin("builtIn");
    builtIn.onStop = () -> events.add("builtIn.stop");
    final RecordingPlugin dropped = new RecordingPlugin("dropped");
    dropped.onStart =
        context -> {
          throw new RuntimeException("start failed");
        };
    dropped.onStop = () -> events.add("dropped.stop");
    final BesuPluginContextImpl context = contextWithBuiltIns(true, List.of(builtIn), dropped);
    register(context);
    context.startPlugins();
    context.afterMainLoop();

    context.stopPluginsForRestart();

    assertThat(events).containsExactly("builtIn.stop");
  }

  @Test
  void shutdownDoesNotStopAPluginDroppedWhileStarting() {
    final List<String> events = new ArrayList<>();
    final RecordingPlugin builtIn = new RecordingPlugin("builtIn");
    builtIn.onStop = () -> events.add("builtIn.stop");
    final RecordingPlugin dropped = new RecordingPlugin("dropped");
    dropped.onStart =
        context -> {
          throw new RuntimeException("start failed");
        };
    dropped.onStop = () -> events.add("dropped.stop");
    final BesuPluginContextImpl context = contextWithBuiltIns(true, List.of(builtIn), dropped);
    register(context);
    context.startPlugins();
    context.afterMainLoop();

    context.stopPlugins();

    assertThat(events).containsExactly("builtIn.stop");
  }

  @Test
  void restartKeepsStoppingWhenAPluginFailsToStop() {
    final List<String> events = new ArrayList<>();
    final RecordingPlugin failing = new RecordingPlugin("failing");
    failing.onStop =
        () -> {
          throw new RuntimeException("stop failed");
        };
    final RecordingPlugin other = new RecordingPlugin("other");
    other.onStop = () -> events.add("other.stop");
    other.onRegister = context -> events.add("other.register");
    final BesuPluginContextImpl context = contextWithBuiltIns(false, List.of(failing), other);
    register(context);
    context.startPlugins();
    context.afterMainLoop();
    events.clear();

    context.stopPluginsForRestart();
    context.resetState();
    register(context);

    assertThat(events).containsExactly("other.stop", "other.register");
  }

  @Test
  void restartStopsEachPluginOncePerCycle() {
    final List<String> events = new ArrayList<>();
    final RecordingPlugin builtIn = new RecordingPlugin("builtIn");
    builtIn.onStop = () -> events.add("builtIn.stop");
    final BesuPluginContextImpl context = contextWithBuiltIns(false, List.of(builtIn));
    register(context);
    context.startPlugins();
    context.afterMainLoop();

    // a second stop before the next cycle starts finds nothing left to stop
    context.stopPluginsForRestart();
    context.stopPluginsForRestart();
    assertThat(events).containsExactly("builtIn.stop");

    // the next cycle's stop stops what that cycle started
    context.resetState();
    register(context);
    context.startPlugins();
    context.afterMainLoop();
    context.stopPluginsForRestart();
    assertThat(events).containsExactly("builtIn.stop", "builtIn.stop");
  }

  @Test
  void restartIsRejectedBeforeThePluginsRegister() {
    final BesuPluginContextImpl context =
        contextWithBuiltIns(false, List.of(new RecordingPlugin("builtIn")));

    assertThatThrownBy(context::stopPluginsForRestart)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("OPTIONS_DEFINED");
  }

  @Test
  void restartIsRejectedWhileRegistrationIsOpen() {
    final BesuPluginContextImpl context =
        contextWithBuiltIns(false, List.of(new RecordingPlugin("builtIn")));
    context.beginRegistration();

    assertThatThrownBy(context::stopPluginsForRestart)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("REGISTERING");
  }

  @Test
  void builtInPluginsCannotBeAddedOnceTheOptionsAreDefined() {
    final BesuPluginContextImpl context = contextWithBuiltIns(false, List.of());

    assertThatThrownBy(() -> context.addBuiltInPlugin(new RecordingPlugin("late")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("before the plugins define their options");
  }

  @Test
  void aBuiltInPluginIsAddedOnce() {
    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    final RecordingPlugin plugin = new RecordingPlugin("twice");
    context.addBuiltInPlugin(plugin);

    assertThatThrownBy(() -> context.addBuiltInPlugin(plugin))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("already added");
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
        .isInstanceOf(UnrecoverablePluginException.class)
        .hasMessageContaining(RecordingPlugin.class.getName())
        .hasMessageContaining("register()");
    assertThatThrownBy(
            () ->
                stashed.get(0).registerService(TestPluginService.class, new TestPluginService() {}))
        .isInstanceOf(RegistrationClosedException.class)
        .hasMessageContaining("TestPluginService");
  }

  @Test
  void staleRegistrationContextLookupIsFatalEvenWithContinueOnError() {
    final List<RegistrationContext> stashed = new ArrayList<>();
    final RecordingPlugin plugin = new RecordingPlugin("stasher");
    plugin.onRegister = stashed::add;
    plugin.onStart = context -> stashed.get(0).getBesuService(TestRegistrationService.class);
    // fatal even when the operator chose to continue on plugin errors
    final BesuPluginContextImpl context = contextReadyToRegister(true, plugin);
    context.addService(TestRegistrationService.class, new TestRegistrationService() {});
    register(context);

    assertThatThrownBy(context::startPlugins)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(RecordingPlugin.class.getName())
        .hasMessageContaining("used its RegistrationContext after register() returned")
        .hasCauseInstanceOf(UnrecoverablePluginException.class);
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
        // the operator sees why the plugin failed, not only which plugin it was
        .hasMessageContaining("not provided on this node")
        .cause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(RecordingPlugin.class.getName())
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
  void pluginMissingItsOwnDependencyIsNotToldToRebuildAgainstTheApi() {
    final RecordingPlugin plugin = new RecordingPlugin("missing-dependency");
    plugin.onRegister =
        context -> {
          throw new NoClassDefFoundError("com/acme/Lib");
        };
    final BesuPluginContextImpl context = contextReadyToRegister(false, plugin);
    context.beginRegistration();

    assertThatThrownBy(context::registerPlugins)
        .hasMessageContaining("needs class com/acme/Lib")
        .hasMessageNotContaining("not compatible with the plugin API");
  }

  @Test
  void pluginMissingABesuClassIsReportedAsIncompatible() {
    final RecordingPlugin plugin = new RecordingPlugin("missing-besu-class");
    plugin.onRegister =
        context -> {
          throw new NoClassDefFoundError("org/hyperledger/besu/plugin/ServiceManager");
        };
    final BesuPluginContextImpl context = contextReadyToRegister(false, plugin);
    context.beginRegistration();

    assertThatThrownBy(context::registerPlugins)
        .hasMessageContaining("not compatible with the plugin API of this Besu version");
  }

  @Test
  void pluginWhoseStaticInitializerFailedGetsThePlainError() {
    final RecordingPlugin plugin = new RecordingPlugin("broken-initializer");
    plugin.onRegister =
        context -> {
          throw new ExceptionInInitializerError(new IllegalStateException("bad config"));
        };
    final BesuPluginContextImpl context = contextReadyToRegister(false, plugin);
    context.beginRegistration();

    assertThatThrownBy(context::registerPlugins)
        .hasMessageContaining("Error registering plugin of type")
        .hasMessageNotContaining("not compatible with the plugin API");
  }

  @Test
  void pluginBuiltAgainstAnIncompatibleApiIsSkippedWhenContinueOnErrorIsSet() {
    final RecordingPlugin outdated = new RecordingPlugin("outdated");
    outdated.onRegister =
        context -> {
          throw new AbstractMethodError("register(RegistrationContext)");
        };
    final OptionPlugin fine = new OptionPlugin();
    final BesuPluginContextImpl context = contextReadyToRegister(true, outdated, fine);

    register(context);
    context.startPlugins();

    assertThat(context.getRegisteredPlugins()).containsExactly(fine);
    assertThat(context.getPluginVersions())
        .containsKey(OptionPlugin.class.getName())
        .doesNotContainKey(RecordingPlugin.class.getName());
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

  /** Distinct plugin classes, so a message can be checked for naming one plugin and not another. */
  static class FirstPublisherPlugin extends RecordingPlugin {
    FirstPublisherPlugin() {
      super("first");
    }
  }

  static class SecondPublisherPlugin extends RecordingPlugin {
    SecondPublisherPlugin() {
      super("second");
    }
  }

  @Test
  void registeringATypeTwiceFailsStartupNamingBothPlugins() {
    final RecordingPlugin first = new FirstPublisherPlugin();
    first.onRegister =
        context -> context.registerService(TestPluginService.class, new TestPluginService() {});
    final RecordingPlugin second = new SecondPublisherPlugin();
    second.onRegister =
        context -> context.registerService(TestPluginService.class, new TestPluginService() {});
    // fatal even when the operator chose to continue on plugin errors
    final BesuPluginContextImpl context = contextReadyToRegister(true, first, second);
    context.beginRegistration();

    // the plugin that registered second is the one failing; the message also names the owner
    assertThatThrownBy(context::registerPlugins)
        .isInstanceOf(RuntimeException.class)
        .hasMessageStartingWith("Plugin " + SecondPublisherPlugin.class.getName() + ":")
        .hasMessageContaining("TestPluginService")
        .hasMessageContaining(
            "plugin " + FirstPublisherPlugin.class.getName() + " already registered")
        .hasCauseInstanceOf(UnrecoverablePluginException.class);
  }

  @Test
  void pluginDroppedAtRegisterGivesUpWhatItPublishedSoAnotherPluginCanPublishIt() {
    final RecordingPlugin dropped = new FirstPublisherPlugin();
    dropped.onRegister =
        context -> {
          context.registerService(TestPluginService.class, new TestPluginService() {});
          throw new IllegalArgumentException("invalid configuration");
        };
    final TestPluginService secondService = new TestPluginService() {};
    final RecordingPlugin second = new SecondPublisherPlugin();
    second.onRegister = context -> context.registerService(TestPluginService.class, secondService);
    final BesuPluginContextImpl context = contextReadyToRegister(true, dropped, second);

    register(context);

    assertThat(context.getRegisteredPlugins()).containsExactly(second);
    assertThat(context.pluginProvidedServices().lookup(TestPluginService.class))
        .containsSame(secondService);
  }

  @Test
  void serviceOfAPluginDroppedAtRegisterIsNotHandedToOtherPlugins() {
    final RecordingPlugin dropped = new RecordingPlugin("dropped");
    dropped.onRegister =
        context -> {
          context.registerService(TestPluginService.class, new TestPluginService() {});
          throw new IllegalArgumentException("invalid configuration");
        };
    final List<Boolean> seen = new ArrayList<>();
    final RecordingPlugin consumer = new RecordingPlugin("consumer");
    consumer.onStart =
        context -> seen.add(context.getPluginService(TestPluginService.class).isPresent());
    final BesuPluginContextImpl context = contextReadyToRegister(true, dropped, consumer);

    register(context);
    context.startPlugins();

    assertThat(seen).containsExactly(false);
    assertThat(context.pluginProvidedServices().lookup(TestPluginService.class)).isEmpty();
  }

  static class TestPluginServiceImpl implements TestPluginService {}

  @Test
  void registeringAClassInsteadOfAnInterfaceFailsOnlyThatPluginWithContinueOnError() {
    final RecordingPlugin broken = new RecordingPlugin("class-not-interface");
    broken.onRegister =
        context ->
            context.registerService(TestPluginServiceImpl.class, new TestPluginServiceImpl());
    final RecordingPlugin fine = new RecordingPlugin("fine");
    final BesuPluginContextImpl context = contextReadyToRegister(true, broken, fine);

    register(context);

    assertThat(context.getRegisteredPlugins()).containsExactly(fine);
  }

  @Test
  void registeringAnObjectThatDoesNotImplementTheTypeFailsStartupByDefault() {
    final RecordingPlugin broken = new RecordingPlugin("wrong-object");
    // null stands for an object that does not implement the type: it fails the same check
    broken.onRegister = context -> context.registerService(TestPluginService.class, null);
    final BesuPluginContextImpl context = contextReadyToRegister(false, broken);
    context.beginRegistration();

    assertThatThrownBy(context::registerPlugins)
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class);
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

    // Pre-register the services the readers look up: one stays, the writers overwrite the other
    final TestStartService service = new TestStartService() {};
    context.addService(TestStartService.class, service);
    context.addService(TestRunningService.class, new TestRunningService() {});
    final RunningContext runningContext = context.runningContextFor(plugin);

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
                      // Writer thread: repeatedly overwrite a service
                      context.addService(TestRunningService.class, new TestRunningService() {});
                    } else {
                      // Reader thread: read the stable service and the one being overwritten
                      startContext.getBesuService(TestStartService.class);
                      runningContext.getBesuService(TestRunningService.class);
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
    assertThat(context.getPluginVersions()).containsKey(OptionPlugin.class.getName());
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
  void currentRegistrantIsThePluginInsideItsRegisterAndEmptyOutside() {
    final List<Optional<String>> seen = new ArrayList<>();
    final RecordingPlugin plugin = new RecordingPlugin("plugin");
    final BesuPluginContextImpl context = contextReadyToRegister(false, plugin);
    plugin.onRegister = registrationContext -> seen.add(context.currentRegistrant());

    assertThat(context.currentRegistrant()).isEmpty();
    context.beginRegistration();
    assertThat(context.currentRegistrant()).isEmpty();
    context.registerPlugins();
    assertThat(context.currentRegistrant()).isEmpty();
    context.endRegistration();

    assertThat(seen).containsExactly(Optional.of(RecordingPlugin.class.getName()));
  }

  @Test
  void currentRegistrantIsClearedWhenAPluginFailsToRegister() {
    final BesuPluginContextImpl[] holder = new BesuPluginContextImpl[1];
    final List<Optional<String>> seenByGood = new ArrayList<>();
    final RecordingPlugin good = new RecordingPlugin("good");
    good.onRegister = registrationContext -> seenByGood.add(holder[0].currentRegistrant());
    holder[0] = contextReadyToRegister(true, new FailingRegisterPlugin(), good);

    register(holder[0]);

    assertThat(seenByGood).containsExactly(Optional.of(RecordingPlugin.class.getName()));
    assertThat(holder[0].currentRegistrant()).isEmpty();
  }

  @Test
  void pluginFailingInRegisterIsNotListedWhenContinueOnErrorIsSet() {
    final CommandLine commandLine = new CommandLine(CommandSpec.create());
    final PicoCLIOptionsImpl picoCLIOptions = new PicoCLIOptionsImpl(commandLine);
    final FailingRegisterPlugin failing = new FailingRegisterPlugin();
    final OptionPlugin good = new OptionPlugin();
    final BesuPluginContextImpl context =
        contextWithLoadedPlugins(picoCLIOptions, true, failing, good);
    assertThat(context.getPluginVersions()).containsKey(FailingRegisterPlugin.class.getName());

    picoCLIOptions.optionsDefinitionCompleted();
    commandLine.parseArgs();
    register(context);

    assertThat(context.getRegisteredPlugins()).containsExactly(good);
    assertThat(context.getPluginVersions())
        .containsKey(OptionPlugin.class.getName())
        .doesNotContainKey(FailingRegisterPlugin.class.getName());
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
  void pluginFailingInAfterMainLoopIsDroppedWhenContinueOnErrorIsSet() {
    final RecordingPlugin failing = new RecordingPlugin("failing");
    failing.onAfterMainLoop =
        context -> {
          throw new IllegalStateException("not ready");
        };
    final RecordingPlugin fine = new RecordingPlugin("fine");
    final BesuPluginContextImpl context = contextReadyToRegister(true, failing, fine);
    register(context);
    context.startPlugins();

    context.afterMainLoop();

    assertThat(context.getRegisteredPlugins()).containsExactly(fine);
  }

  @Test
  void pluginFailingInAfterMainLoopFailsStartupByDefault() {
    final RecordingPlugin failing = new RecordingPlugin("failing");
    failing.onAfterMainLoop =
        context -> {
          throw new IllegalStateException("not ready");
        };
    final BesuPluginContextImpl context = contextReadyToRegister(false, failing);
    register(context);
    context.startPlugins();

    assertThatThrownBy(context::afterMainLoop)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(RecordingPlugin.class.getName())
        .hasMessageContaining("not ready")
        .hasCauseInstanceOf(IllegalStateException.class);
  }

  @Test
  void registrationFromAfterMainLoopIsFatalEvenWithContinueOnError() {
    final List<HealthCheckService> stashed = new ArrayList<>();
    final RecordingPlugin plugin = new RecordingPlugin("late");
    plugin.onRegister = context -> stashed.add(context.getBesuService(HealthCheckService.class));
    plugin.onAfterMainLoop =
        context ->
            stashed
                .get(0)
                .registerHealthCheck(
                    "/late", params -> HealthCheckService.HealthCheckResult.of(true));
    final BesuPluginContextImpl context = contextReadyToRegister(true, plugin);
    final HealthCheckServiceImpl healthCheckService =
        new HealthCheckServiceImpl(context::isRegistering);
    context.addService(HealthCheckService.class, healthCheckService);
    register(context);
    context.startPlugins();

    assertThatThrownBy(context::afterMainLoop)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(RecordingPlugin.class.getName())
        .hasMessageContaining("registerHealthCheck")
        .hasCauseInstanceOf(RegistrationClosedException.class);
    assertThat(healthCheckService.getHealthCheck("/late")).isEmpty();
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

  /** Holders of plugins with the same simple class name, loadable by the service loader. */
  public static class First {
    public static class Plugin extends RecordingPlugin {
      public Plugin() {
        super("first");
      }
    }
  }

  public static class Second {
    public static class Plugin extends RecordingPlugin {
      public Plugin() {
        super("second");
      }
    }
  }

  public static class Third {
    public static class Plugin extends RecordingPlugin {
      public Plugin() {
        super("third");
      }
    }
  }

  public static class Fourth {
    public static class Plugin extends RecordingPlugin {
      public Plugin() {
        super("fourth");
      }
    }
  }

  @Test
  void pluginsAreIdentifiedByTheirFullyQualifiedClassName() {
    final OptionPlugin plugin = new OptionPlugin();
    final BesuPluginContextImpl context = contextReadyToRegister(false, plugin);
    register(context);

    assertThat(context.getPluginVersions()).containsOnlyKeys(OptionPlugin.class.getName());
    assertThat(context.getPluginsById()).containsOnlyKeys(OptionPlugin.class.getName());
    assertThat(context.getPluginsSummaryLog())
        .containsExactly(
            "Plugin Registration Summary:",
            "Registered Plugins:",
            " - " + OptionPlugin.class.getName() + " (" + plugin.getVersion() + ")",
            "TOTAL = 1 of 1 plugins successfully registered.");
  }

  @Test
  void requestedPluginsAreSelectedByIdOrSimpleClassNameAndKeepTheDetectionOrder() {
    final OptionPlugin option = new OptionPlugin();
    final First.Plugin first = new First.Plugin();
    final RecordingPlugin other = new RecordingPlugin("other");
    final BesuPluginContextImpl context =
        contextSelecting(
            false,
            List.of(First.Plugin.class.getName(), "OptionPlugin"),
            List.of(providerOf(option), providerOf(other), providerOf(first)));
    register(context);

    assertThat(context.getRegisteredPlugins()).containsExactly(option, first);
    assertThat(context.getPluginsSummaryLog())
        .containsExactly(
            "Plugin Registration Summary:",
            "Registered Plugins:",
            " - " + OptionPlugin.class.getName() + " (" + option.getVersion() + ")",
            " - " + First.Plugin.class.getName() + " (" + first.getVersion() + ")",
            "Detected but not selected:",
            " - " + RecordingPlugin.class.getName(),
            "TOTAL = 2 of 3 plugins successfully registered.");
  }

  @Test
  void pluginsLeftOutOfTheSelectionAreNotInstantiated() {
    final OptionPlugin option = new OptionPlugin();
    final AtomicBoolean constructed = new AtomicBoolean();
    final ServiceLoader.Provider<BesuPlugin> excluded =
        new ServiceLoader.Provider<>() {
          @Override
          public Class<? extends BesuPlugin> type() {
            return RecordingPlugin.class;
          }

          @Override
          public BesuPlugin get() {
            constructed.set(true);
            return new RecordingPlugin("excluded");
          }
        };
    final BesuPluginContextImpl context =
        contextSelecting(false, List.of("OptionPlugin"), List.of(excluded, providerOf(option)));
    register(context);

    assertThat(constructed).isFalse();
    assertThat(context.getRegisteredPlugins()).containsExactly(option);
  }

  @Test
  void simpleClassNameNamingSeveralPluginsIsRejectedListingTheirIds() {
    final List<ServiceLoader.Provider<BesuPlugin>> detected =
        List.of(providerOf(new First.Plugin()), providerOf(new Second.Plugin()));

    assertThatThrownBy(() -> contextSelecting(false, List.of("Plugin"), detected))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Plugin name Plugin matches several plugins, select one by its id: "
                + First.Plugin.class.getName()
                + ", "
                + Second.Plugin.class.getName());
  }

  @Test
  void idSelectsOneOfSeveralPluginsWithTheSameSimpleClassName() {
    final Second.Plugin second = new Second.Plugin();
    final BesuPluginContextImpl context =
        contextSelecting(
            false,
            List.of(Second.Plugin.class.getName()),
            List.of(providerOf(new First.Plugin()), providerOf(second)));
    register(context);

    assertThat(context.getRegisteredPlugins()).containsExactly(second);
  }

  @Test
  void requestedPluginThatWasNotDetectedFailsStartup() {
    final List<ServiceLoader.Provider<BesuPlugin>> detected =
        List.of(providerOf(new OptionPlugin()));

    assertThatThrownBy(() -> contextSelecting(false, List.of("OptionPlugin", "Missing"), detected))
        .isInstanceOf(NoSuchElementException.class)
        .hasMessage("The following requested plugins were not found: Missing");
  }

  @Test
  void pluginWhoseConstructorFailsFailsStartupByDefault() {
    final List<ServiceLoader.Provider<BesuPlugin>> detected =
        List.of(failingProviderOf(RecordingPlugin.class, new IllegalStateException("boom")));

    assertThatThrownBy(() -> contextSelecting(false, List.of(), detected))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(
            "Error instantiating plugin of type " + RecordingPlugin.class.getName())
        .hasMessageContaining("boom");
  }

  @Test
  void pluginWhoseStaticInitializerFailsReportsTheRootCause() {
    final List<ServiceLoader.Provider<BesuPlugin>> detected =
        List.of(
            failingProviderOf(
                RecordingPlugin.class,
                new ExceptionInInitializerError(new IllegalStateException("config missing"))));

    assertThatThrownBy(() -> contextSelecting(false, List.of(), detected))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(
            "Error instantiating plugin of type " + RecordingPlugin.class.getName())
        .hasMessageContaining("config missing");
  }

  @Test
  void pluginWhoseConstructorFailsIsSkippedWhenContinueOnErrorIsSet() {
    final OptionPlugin option = new OptionPlugin();
    final BesuPluginContextImpl context =
        contextSelecting(
            true,
            List.of(),
            List.of(
                failingProviderOf(RecordingPlugin.class, new IllegalStateException("boom")),
                providerOf(option)));
    register(context);

    assertThat(context.getRegisteredPlugins()).containsExactly(option);
    assertThat(context.getPluginsSummaryLog())
        .containsSubsequence(
            "Detected but not registered:",
            " - " + RecordingPlugin.class.getName(),
            "TOTAL = 1 of 2 plugins successfully registered.");
  }

  @Test
  void pluginWhoseClassCannotBeLoadedFailsStartupNamingItsJarAndTheMissingClass() {
    final List<ServiceLoader.Provider<BesuPlugin>> detected =
        List.of(providerOf(new OptionPlugin()));

    assertThatThrownBy(
            () -> contextSelecting(false, List.of(), detected, List.of(pluginMissingItsLibrary())))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(
            "Plugin of type com.acme.BridgePlugin (declared by bridge.jar) needs class"
                + " com/acme/LibBase, which is not available; a dependency may be missing");
  }

  @Test
  void pluginWhoseClassNeedsAMissingBesuClassIsReportedAsIncompatible() {
    final BesuPluginContextImpl.UnloadablePlugin outdated =
        new BesuPluginContextImpl.UnloadablePlugin(
            "com.acme.OutdatedPlugin",
            Path.of("plugins", "outdated.jar"),
            new NoClassDefFoundError("org/hyperledger/besu/plugin/services/GoneService"));

    assertThatThrownBy(() -> contextSelecting(false, List.of(), List.of(), List.of(outdated)))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(
            "Plugin of type com.acme.OutdatedPlugin (declared by outdated.jar) is not compatible"
                + " with the plugin API of this Besu version");
  }

  @Test
  void pluginWhoseClassCannotBeLoadedIsIgnoredWhenLeftOutOfTheSelection() {
    final OptionPlugin option = new OptionPlugin();
    final BesuPluginContextImpl context =
        contextSelecting(
            false,
            List.of("OptionPlugin"),
            List.of(providerOf(option)),
            List.of(pluginMissingItsLibrary()));
    register(context);

    assertThat(context.getRegisteredPlugins()).containsExactly(option);
    assertThat(context.getPluginsSummaryLog())
        .containsSubsequence(
            "Detected but not selected:",
            " - com.acme.BridgePlugin (could not be loaded)",
            "TOTAL = 1 of 2 plugins successfully registered.");
  }

  @Test
  void pluginWhoseClassCannotBeLoadedIsStillSelectedBySimpleClassName() {
    final List<ServiceLoader.Provider<BesuPlugin>> detected =
        List.of(providerOf(new OptionPlugin()));

    // it is a selected plugin that fails, not a requested name that was not found
    assertThatThrownBy(
            () ->
                contextSelecting(
                    false, List.of("BridgePlugin"), detected, List.of(pluginMissingItsLibrary())))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Plugin of type com.acme.BridgePlugin (declared by bridge.jar)");
  }

  @Test
  void pluginWhoseClassCannotBeLoadedIsSkippedWhenContinueOnErrorIsSet() {
    final OptionPlugin option = new OptionPlugin();
    final BesuPluginContextImpl context =
        contextSelecting(
            true, List.of(), List.of(providerOf(option)), List.of(pluginMissingItsLibrary()));
    register(context);

    assertThat(context.getRegisteredPlugins()).containsExactly(option);
    assertThat(context.getPluginsSummaryLog())
        .containsSubsequence(
            "Detected but not registered:",
            " - com.acme.BridgePlugin (could not be loaded)",
            "TOTAL = 1 of 2 plugins successfully registered.");
  }

  @Test
  void jarsAreLoadedInFileNameOrder(@TempDir final Path tempDir) throws IOException {
    final Path pluginsDir = Files.createDirectory(tempDir.resolve("plugins"));
    // written in the reverse of their file name order; with four jars, an unsorted directory
    // listing matches the expected order only by a remote chance
    writeJarDeclaring(pluginsDir.resolve("d.jar"), Fourth.Plugin.class.getName());
    writeJarDeclaring(pluginsDir.resolve("c.jar"), Third.Plugin.class.getName());
    writeJarDeclaring(pluginsDir.resolve("b.jar"), Second.Plugin.class.getName());
    writeJarDeclaring(pluginsDir.resolve("a.jar"), First.Plugin.class.getName());
    final BesuPluginContextImpl context = contextScanning(pluginsDir);

    context.defineOptions(freshPicoCLIOptions());

    assertThat(context.getPluginsSummaryLog())
        .containsSubsequence(
            "Detected but not registered:",
            " - " + First.Plugin.class.getName() + " (" + new First.Plugin().getVersion() + ")",
            " - " + Second.Plugin.class.getName() + " (" + new Second.Plugin().getVersion() + ")",
            " - " + Third.Plugin.class.getName() + " (" + new Third.Plugin().getVersion() + ")",
            " - " + Fourth.Plugin.class.getName() + " (" + new Fourth.Plugin().getVersion() + ")",
            "TOTAL = 0 of 4 plugins successfully registered.");
  }

  @Test
  void pluginsOfOneJarAreLoadedInServiceFileOrder(@TempDir final Path tempDir) throws IOException {
    final Path pluginsDir = Files.createDirectory(tempDir.resolve("plugins"));
    // declared in the reverse of their class name order, so a sort by name would show
    writeJarDeclaring(
        pluginsDir.resolve("bundle.jar"),
        Second.Plugin.class.getName(),
        First.Plugin.class.getName());
    final BesuPluginContextImpl context = contextScanning(pluginsDir);

    context.defineOptions(freshPicoCLIOptions());

    assertThat(context.getPluginsSummaryLog())
        .containsSubsequence(
            "Detected but not registered:",
            " - " + Second.Plugin.class.getName() + " (" + new Second.Plugin().getVersion() + ")",
            " - " + First.Plugin.class.getName() + " (" + new First.Plugin().getVersion() + ")",
            "TOTAL = 0 of 2 plugins successfully registered.");
  }

  @Test
  void missingPluginsDirectoryMeansNoPlugins(@TempDir final Path tempDir) {
    final BesuPluginContextImpl context = contextScanning(tempDir.resolve("missing"));

    context.defineOptions(freshPicoCLIOptions());

    assertThat(context.getPluginsSummaryLog())
        .contains("TOTAL = 0 of 0 plugins successfully registered.");
  }

  @Test
  void pluginsPathThatIsNotADirectoryFailsStartupNamingIt(@TempDir final Path tempDir)
      throws IOException {
    final Path file = Files.createFile(tempDir.resolve("plugins"));
    final BesuPluginContextImpl context = contextScanning(file);

    assertThatThrownBy(() -> context.defineOptions(freshPicoCLIOptions()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Plugins directory " + file.toAbsolutePath() + " is not a directory");
  }

  @Test
  void unreadablePluginsDirectoryFailsStartupNamingIt(@TempDir final Path tempDir)
      throws IOException {
    final Path pluginsDir = Files.createDirectory(tempDir.resolve("plugins"));
    assumeTrue(
        Files.getFileStore(pluginsDir).supportsFileAttributeView(PosixFileAttributeView.class));
    Files.setPosixFilePermissions(pluginsDir, Set.of());
    try {
      assumeTrue(!Files.isReadable(pluginsDir), "this user reads the directory regardless");
      final BesuPluginContextImpl context = contextScanning(pluginsDir);

      assertThatThrownBy(() -> context.defineOptions(freshPicoCLIOptions()))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Cannot read the plugins directory " + pluginsDir.toAbsolutePath());
    } finally {
      Files.setPosixFilePermissions(pluginsDir, PosixFilePermissions.fromString("rwxr-xr-x"));
    }
  }

  @Test
  void pluginsDirectoryBehindAnInaccessibleParentFailsStartupSayingSo(@TempDir final Path tempDir)
      throws IOException {
    final Path parent = Files.createDirectory(tempDir.resolve("parent"));
    final Path pluginsDir = Files.createDirectory(parent.resolve("plugins"));
    assumeTrue(Files.getFileStore(parent).supportsFileAttributeView(PosixFileAttributeView.class));
    Files.setPosixFilePermissions(parent, Set.of());
    try {
      assumeTrue(
          !Files.exists(pluginsDir) && !Files.notExists(pluginsDir),
          "this user traverses the directory regardless");
      final BesuPluginContextImpl context = contextScanning(pluginsDir);

      assertThatThrownBy(() -> context.defineOptions(freshPicoCLIOptions()))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Cannot access the plugins directory " + pluginsDir.toAbsolutePath());
    } finally {
      Files.setPosixFilePermissions(parent, PosixFilePermissions.fromString("rwxr-xr-x"));
    }
  }

  @Test
  void declaredPluginWhoseClassIsMissingFailsStartupNamingItAndItsJar(@TempDir final Path tempDir)
      throws IOException {
    final Path pluginsDir = Files.createDirectory(tempDir.resolve("plugins"));
    writeJarDeclaring(pluginsDir.resolve("broken.jar"), "com.acme.MissingPlugin");
    final BesuPluginContextImpl context = contextScanning(pluginsDir);

    assertThatThrownBy(() -> context.defineOptions(freshPicoCLIOptions()))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(
            "Error loading plugin of type com.acme.MissingPlugin (declared by broken.jar)")
        .hasMessageContaining("ClassNotFoundException");
  }

  @Test
  void discoveryCarriesOnPastADeclaredPluginWhoseClassIsMissing(@TempDir final Path tempDir)
      throws IOException {
    final Path pluginsDir = Files.createDirectory(tempDir.resolve("plugins"));
    // the broken declaration comes first, in the first jar, ahead of the two plugins that load
    writeJarDeclaring(
        pluginsDir.resolve("a.jar"), "com.acme.MissingPlugin", First.Plugin.class.getName());
    writeJarDeclaring(pluginsDir.resolve("b.jar"), Second.Plugin.class.getName());
    final BesuPluginContextImpl context =
        contextScanning(
            pluginsDir, List.of(First.Plugin.class.getName(), Second.Plugin.class.getName()));

    context.defineOptions(freshPicoCLIOptions());

    assertThat(context.getPluginsSummaryLog())
        .containsSubsequence(
            "Detected but not registered:",
            " - " + First.Plugin.class.getName() + " (" + new First.Plugin().getVersion() + ")",
            " - " + Second.Plugin.class.getName() + " (" + new Second.Plugin().getVersion() + ")",
            "Detected but not selected:",
            " - com.acme.MissingPlugin (could not be loaded)",
            "TOTAL = 0 of 3 plugins successfully registered.");
  }

  @Test
  void serviceFileWithAMalformedLineFailsStartupNamingThePluginItHidesAndTheLine(
      @TempDir final Path tempDir) throws IOException {
    final Path pluginsDir = Files.createDirectory(tempDir.resolve("plugins"));
    // the loader rejects the whole file for one bad line, so the plugin before it never arrives
    final Path jar = pluginsDir.resolve("bundle.jar");
    writeJarDeclaring(jar, First.Plugin.class.getName(), "not a class name");
    final BesuPluginContextImpl context = contextScanning(pluginsDir);

    assertThatThrownBy(() -> context.defineOptions(freshPicoCLIOptions()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith(
            "Plugin "
                + First.Plugin.class.getName()
                + " declared by "
                + jar.toAbsolutePath()
                + " was not discovered by the service loader, which reported: ")
        .hasMessageContaining(":2: Illegal configuration-file syntax");
  }

  @Test
  void jarThatCannotBeOpenedFailsStartupNamingIt(@TempDir final Path tempDir) throws IOException {
    final Path pluginsDir = Files.createDirectory(tempDir.resolve("plugins"));
    writeJarDeclaring(pluginsDir.resolve("a.jar"), First.Plugin.class.getName());
    final Path corrupt = Files.writeString(pluginsDir.resolve("b.jar"), "not a zip file");
    final BesuPluginContextImpl context = contextScanning(pluginsDir);

    assertThatThrownBy(() -> context.defineOptions(freshPicoCLIOptions()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot open the plugin jar " + corrupt.toAbsolutePath());
  }
}
