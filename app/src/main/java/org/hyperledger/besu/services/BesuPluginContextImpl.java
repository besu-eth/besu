/*
 * Copyright ConsenSys AG.
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.ethereum.core.plugins.PluginProvidedServices;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.RegistrationClosedException;
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.RegistrationService;
import org.hyperledger.besu.plugin.RunningContext;
import org.hyperledger.besu.plugin.RunningService;
import org.hyperledger.besu.plugin.StartContext;
import org.hyperledger.besu.plugin.StartService;
import org.hyperledger.besu.plugin.services.BesuService;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.PluginVersionsProvider;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Besu plugin context implementation: the service registry and the driver of the plugin
 * lifecycle.
 *
 * <p>The registry holds two kinds of services. Besu-provided services are added by Besu through
 * {@link #addService(Class, BesuService)} when their dependencies exist, and are handed to plugins
 * through the per-phase contexts, whose lookups are bounded by the service tier markers.
 * Plugin-provided services are published by plugins through {@link
 * RegistrationContext#registerService(Class, BesuService)} while registration is open, and are read
 * by Besu through {@link #pluginProvidedServices()} and by other plugins through the {@code
 * getPluginService} lookups.
 */
// PluginVersionsProvider is deprecated for removal from the plugin API
@SuppressWarnings("removal")
public class BesuPluginContextImpl implements PluginVersionsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BesuPluginContextImpl.class);

  private enum Lifecycle {
    /** Uninitialized lifecycle. */
    UNINITIALIZED,
    /** Initialized lifecycle. */
    INITIALIZED,
    /** Plugins are being loaded and are declaring their CLI options. */
    DEFINING_OPTIONS,
    /** Every plugin has declared its CLI options; the command line can be parsed. */
    OPTIONS_DEFINED,
    /** Registration is open: plugins are registering. */
    REGISTERING,
    /** Registration is closed: what plugins registered is final. */
    REGISTERED,
    /** Before main loop started lifecycle. */
    BEFORE_MAIN_LOOP_STARTED,
    /** Before main loop finished lifecycle. */
    BEFORE_MAIN_LOOP_FINISHED,
    /** Stopping lifecycle. */
    STOPPING,
    /** Stopped lifecycle. */
    STOPPED
  }

  /** A service published by a plugin, with the plugin that published it. */
  private record ServiceWithProvider(BesuPlugin provider, BesuService service) {}

  private volatile Lifecycle state = Lifecycle.UNINITIALIZED;
  private final Map<Class<?>, BesuService> besuServices = new ConcurrentHashMap<>();
  private final Map<Class<?>, ServiceWithProvider> pluginServices = new ConcurrentHashMap<>();

  /** The ids of every plugin found in the plugins directory, in load order. */
  private List<String> detectedPluginIds = List.of();

  /** The ids of the detected plugins that the configuration selects, in load order. */
  private List<String> selectedPluginIds = List.of();

  /** The selected plugins that were instantiated, by id, in load order. */
  private final Map<String, BesuPlugin> instantiatedPlugins = new LinkedHashMap<>();

  /** Plugins that completed {@code defineOptions()}, in load order. */
  private final List<BesuPlugin> loadedPlugins = new ArrayList<>();

  private final List<BesuPlugin> registeredPlugins = new ArrayList<>();

  private final Map<String, String> pluginVersions = new LinkedHashMap<>();
  private PluginConfiguration config;
  private URLClassLoader pluginClassLoader;

  /** Instantiates a new Besu plugin context. */
  public BesuPluginContextImpl() {}

  /**
   * Adds a Besu-provided service. Called by Besu when the service's dependencies exist, before the
   * phase whose tier marker the service carries.
   *
   * @param <T> the service type
   * @param serviceType the service interface
   * @param service the implementation
   */
  public <T extends BesuService> void addService(final Class<T> serviceType, final T service) {
    checkArgument(serviceType.isInterface(), "Services must be Java interfaces.");
    checkArgument(
        serviceType.isInstance(service),
        "The service registered with a type must implement that type");
    besuServices.put(serviceType, service);
  }

  /**
   * Whether plugin registration is open, that is between {@link #beginRegistration()} and {@link
   * #endRegistration()}. The registration-tier services consult this to reject late registrations.
   *
   * @return true while registration is open
   */
  public boolean isRegistering() {
    return state == Lifecycle.REGISTERING;
  }

  /**
   * The services plugins published during registration, for Besu's own code to consume. The view is
   * complete once registration has closed.
   *
   * @return the plugin-provided services view
   */
  public PluginProvidedServices pluginProvidedServices() {
    checkState(
        state != Lifecycle.REGISTERING,
        "Plugin-provided services cannot be read while registration is open");
    return new PluginProvidedServices() {
      @Override
      public <T extends BesuService> Optional<T> lookup(final Class<T> type) {
        return lookupPluginService(type);
      }
    };
  }

  private <T extends BesuService> Optional<T> lookupPluginService(final Class<T> type) {
    return Optional.ofNullable(pluginServices.get(type)).map(entry -> type.cast(entry.service()));
  }

  private static boolean isBesuProvidedType(final Class<?> type) {
    return RegistrationService.class.isAssignableFrom(type)
        || RunningService.class.isAssignableFrom(type);
  }

  private <T extends BesuService> T besuService(final String pluginName, final Class<T> type) {
    final BesuService service = besuServices.get(type);
    if (service == null) {
      throw new IllegalStateException(
          "Plugin "
              + pluginName
              + " requested "
              + type.getSimpleName()
              + ", which is not provided on this node; see the service's documentation for the"
              + " node configurations that provide it");
    }
    return type.cast(service);
  }

  private <T extends BesuService> Optional<T> pluginService(final Class<T> type) {
    checkArgument(
        !isBesuProvidedType(type),
        "%s is a Besu-provided service, not a plugin-provided one; look it up with"
            + " getBesuService() from the context of a phase in which it is available",
        type.getSimpleName());
    return lookupPluginService(type);
  }

  /**
   * The registration-phase context for the given plugin, valid only while registration is open.
   *
   * @param plugin the plugin the context is for
   * @return the context
   */
  public RegistrationContext registrationContextFor(final BesuPlugin plugin) {
    final String pluginName = pluginId(plugin);
    return new RegistrationContext() {
      @Override
      public <T extends RegistrationService> T getBesuService(final Class<T> type) {
        if (!isRegistering()) {
          throw new UnrecoverablePluginException(
              "Plugin "
                  + pluginName
                  + " used its RegistrationContext after register() returned; registration-phase"
                  + " services are only available from BesuPlugin.register()");
        }
        return besuService(pluginName, type);
      }

      @Override
      public <T extends BesuService> void registerService(final Class<T> type, final T service) {
        if (!isRegistering()) {
          throw new RegistrationClosedException(
              "RegistrationContext", "registerService(" + type.getSimpleName() + ")");
        }
        checkArgument(type.isInterface(), "Services must be Java interfaces.");
        checkArgument(
            type.isInstance(service),
            "The service registered with a type must implement that type");
        if (isBesuProvidedType(type) || besuServices.containsKey(type)) {
          throw new UnrecoverablePluginException(
              "cannot register "
                  + type.getSimpleName()
                  + ": it is a Besu-provided service and cannot be replaced by a plugin");
        }
        final ServiceWithProvider previous =
            pluginServices.putIfAbsent(type, new ServiceWithProvider(plugin, service));
        if (previous != null) {
          throw new UnrecoverablePluginException(
              "cannot register "
                  + type.getSimpleName()
                  + ": plugin "
                  + previous.provider().getClass().getName()
                  + " already registered it");
        }
      }
    };
  }

  /**
   * The start-phase context for the given plugin. The services it returns keep working for the rest
   * of the node's life.
   *
   * @param plugin the plugin the context is for
   * @return the context
   */
  public StartContext startContextFor(final BesuPlugin plugin) {
    final String pluginName = pluginId(plugin);
    return new StartContext() {
      @Override
      public <T extends StartService> T getBesuService(final Class<T> type) {
        return besuService(pluginName, type);
      }

      @Override
      public <T extends BesuService> Optional<T> getPluginService(final Class<T> type) {
        return pluginService(type);
      }
    };
  }

  /**
   * The running-phase context for the given plugin.
   *
   * @param plugin the plugin the context is for
   * @return the context
   */
  public RunningContext runningContextFor(final BesuPlugin plugin) {
    final String pluginName = pluginId(plugin);
    return new RunningContext() {
      @Override
      public <T extends RunningService> T getBesuService(final Class<T> type) {
        return besuService(pluginName, type);
      }

      @Override
      public <T extends BesuService> Optional<T> getPluginService(final Class<T> type) {
        return pluginService(type);
      }
    };
  }

  /**
   * Initializes the plugin context with the provided {@link PluginConfiguration}.
   *
   * @param config the plugin configuration
   * @throws IllegalStateException if the system is not in the UNINITIALIZED state.
   */
  public void initialize(final PluginConfiguration config) {
    checkState(
        state == Lifecycle.UNINITIALIZED,
        "Besu plugins have already been initialized. Cannot initialize again.");
    this.config = config;
    state = Lifecycle.INITIALIZED;
  }

  /**
   * Discovers, verifies and instantiates the plugins selected by the {@link PluginConfiguration},
   * then calls {@link BesuPlugin#defineOptions(PicoCLIOptions)} on each. Runs before the command
   * line is parsed.
   *
   * @param picoCLIOptions the option registry handed to each plugin
   * @throws IllegalStateException if the context is not in the INITIALIZED state.
   */
  public void defineOptions(final PicoCLIOptions picoCLIOptions) {
    checkState(
        state == Lifecycle.INITIALIZED,
        "BesuContext should be in state %s but it was in %s",
        Lifecycle.INITIALIZED,
        state);
    state = Lifecycle.DEFINING_OPTIONS;

    for (final BesuPlugin plugin : loadPlugins()) {
      try {
        plugin.defineOptions(picoCLIOptions);
        pluginVersions.put(pluginId(plugin), plugin.getVersion());
        loadedPlugins.add(plugin);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Defined options of plugin of type {}.", pluginId(plugin));
        }
      } catch (final Exception | LinkageError e) {
        final String failure = failureMessage("defining options of", pluginId(plugin), e);
        if (config.isContinueOnPluginError()) {
          LOG.error("{}, register, start and stop will not be called.", failure, e);
        } else {
          throw new RuntimeException(failure, e);
        }
      }
    }
    state = Lifecycle.OPTIONS_DEFINED;
  }

  /**
   * The id of a plugin: its fully qualified class name, which is what {@code --plugins}, {@code
   * --version}, the startup summary and the reload RPC use.
   */
  private static String pluginId(final BesuPlugin plugin) {
    return pluginId(plugin.getClass());
  }

  private static String pluginId(final Class<?> pluginType) {
    return pluginType.getName();
  }

  /**
   * Discovers and verifies the plugins in the plugins directory, selects the ones the configuration
   * requests and instantiates only those, in load order.
   *
   * @return the plugins to define options for, empty when external plugins are disabled
   */
  private List<BesuPlugin> loadPlugins() {
    if (!config.isExternalPluginsEnabled()) {
      LOG.debug("External plugins are disabled. Skipping plugins loading.");
      return List.of();
    }
    final List<ServiceLoader.Provider<BesuPlugin>> detected = detectPlugins();
    detectedPluginIds = detected.stream().map(provider -> pluginId(provider.type())).toList();
    final List<ServiceLoader.Provider<BesuPlugin>> selected =
        config.getRequestedPlugins().isEmpty()
            ? detected
            : selectRequestedPlugins(config.getRequestedPlugins(), detected);
    selectedPluginIds = selected.stream().map(provider -> pluginId(provider.type())).toList();
    return instantiate(selected);
  }

  /**
   * Resolves each requested plugin to one of the detected plugins: by id first, otherwise by simple
   * class name when exactly one detected plugin has it.
   *
   * @return the selected plugins, in load order rather than in the order they were requested
   * @throws IllegalArgumentException if a name is no plugin's id and is the simple class name of
   *     several detected plugins
   * @throws NoSuchElementException if a requested plugin was not detected
   */
  private static List<ServiceLoader.Provider<BesuPlugin>> selectRequestedPlugins(
      final List<String> requestedPlugins,
      final List<ServiceLoader.Provider<BesuPlugin>> detected) {
    final List<String> detectedIds = detected.stream().map(p -> pluginId(p.type())).toList();
    final Set<String> selectedIds = new HashSet<>();
    final List<String> missing = new ArrayList<>();
    for (final String requested : requestedPlugins) {
      if (detectedIds.contains(requested)) {
        selectedIds.add(requested);
        continue;
      }
      final List<String> matchingIds =
          detected.stream()
              .map(ServiceLoader.Provider::type)
              .filter(pluginType -> requested.equals(pluginType.getSimpleName()))
              .map(BesuPluginContextImpl::pluginId)
              .toList();
      if (matchingIds.size() > 1) {
        throw new IllegalArgumentException(
            "Plugin name "
                + requested
                + " matches several plugins, select one by its id: "
                + String.join(", ", matchingIds));
      }
      if (matchingIds.isEmpty()) {
        missing.add(requested);
      } else {
        selectedIds.add(matchingIds.getFirst());
      }
    }
    if (!missing.isEmpty()) {
      throw new NoSuchElementException(
          "The following requested plugins were not found: " + String.join(", ", missing));
    }
    return detected.stream()
        .filter(provider -> selectedIds.contains(pluginId(provider.type())))
        .toList();
  }

  /**
   * Instantiates the selected plugins. A plugin whose constructor or static initializer fails is
   * skipped with {@code --plugin-continue-on-error}, and fails startup otherwise.
   */
  private List<BesuPlugin> instantiate(final List<ServiceLoader.Provider<BesuPlugin>> selected) {
    final List<BesuPlugin> plugins = new ArrayList<>(selected.size());
    for (final ServiceLoader.Provider<BesuPlugin> provider : selected) {
      final String pluginId = pluginId(provider.type());
      try {
        final BesuPlugin plugin = provider.get();
        plugins.add(plugin);
        instantiatedPlugins.put(pluginId, plugin);
      } catch (final ServiceConfigurationError e) {
        final String failure = failureMessage("instantiating", pluginId, instantiationCause(e));
        if (config.isContinueOnPluginError()) {
          LOG.error("{}, defineOptions, register, start and stop will not be called.", failure, e);
        } else {
          throw new RuntimeException(failure, e);
        }
      }
    }
    return plugins;
  }

  /**
   * The failure behind a plugin that could not be instantiated: the service loader wraps what the
   * constructor threw, and a failing static initializer arrives as an {@link
   * ExceptionInInitializerError} around the real exception.
   */
  private static Throwable instantiationCause(final ServiceConfigurationError e) {
    Throwable cause = e.getCause() == null ? e : e.getCause();
    while (cause instanceof ExceptionInInitializerError && cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }

  /**
   * Opens registration. Built-in plugins driven by the caller register between this call and {@link
   * #endRegistration()}, around {@link #registerPlugins()} for the external plugins. Must run after
   * the command line has been parsed.
   *
   * @throws IllegalStateException if the context is not in the OPTIONS_DEFINED state.
   */
  public void beginRegistration() {
    checkState(
        state == Lifecycle.OPTIONS_DEFINED,
        "BesuContext should be in state %s but it was in %s",
        Lifecycle.OPTIONS_DEFINED,
        state);
    registeredPlugins.clear();
    state = Lifecycle.REGISTERING;
  }

  /**
   * Calls {@link BesuPlugin#register(RegistrationContext)} on every plugin that completed {@link
   * #defineOptions(PicoCLIOptions)}. Runs while registration is open.
   *
   * @throws IllegalStateException if the context is not in the REGISTERING state.
   */
  public void registerPlugins() {
    checkState(
        state == Lifecycle.REGISTERING,
        "BesuContext should be in state %s but it was in %s",
        Lifecycle.REGISTERING,
        state);
    for (final BesuPlugin plugin : loadedPlugins) {
      if (registerPlugin(plugin)) {
        registeredPlugins.add(plugin);
      }
    }
  }

  /**
   * Closes registration. From here on registration-tier services reject registrations, and what
   * plugins registered is what Besu reads.
   *
   * @throws IllegalStateException if the context is not in the REGISTERING state.
   */
  public void endRegistration() {
    checkState(
        state == Lifecycle.REGISTERING,
        "BesuContext should be in state %s but it was in %s",
        Lifecycle.REGISTERING,
        state);
    state = Lifecycle.REGISTERED;
  }

  /**
   * Finds the plugin misuse in the causal chain of a lifecycle failure that is always fatal, even
   * with {@code --plugin-continue-on-error} and even when the plugin wrapped it.
   */
  private static Optional<Throwable> unrecoverableCause(final Throwable e) {
    return Throwables.getCausalChain(e).stream()
        .filter(
            t ->
                t instanceof UnrecoverablePluginException
                    || t instanceof RegistrationClosedException)
        .findFirst();
  }

  /**
   * Describes the failure of a lifecycle call. A linkage error that points at Besu's own classes
   * means the plugin was built against a plugin API this Besu version does not provide, and is
   * reported as such. A class missing from elsewhere usually means a dependency was not shipped
   * with the plugin. Any other failure is reported as a plain error. The message always carries the
   * cause, since the startup path logs only the message at the default level.
   */
  private static String failureMessage(
      final String action, final String pluginType, final Throwable e) {
    if (isPluginApiMismatch(e)) {
      return "Plugin of type "
          + pluginType
          + " is not compatible with the plugin API of this Besu version and must be rebuilt"
          + " against it ("
          + e
          + ")";
    }
    if (isMissingClass(e)) {
      return "Plugin of type "
          + pluginType
          + " needs class "
          + e.getMessage()
          + ", which is not available; a dependency may be missing from the plugin ("
          + e
          + ")";
    }
    return "Error " + action + " plugin of type " + pluginType + " (" + e + ")";
  }

  /** The JVM's "a class changed incompatibly" errors, or a missing Besu class. */
  private static boolean isPluginApiMismatch(final Throwable e) {
    return e instanceof IncompatibleClassChangeError
        || (isMissingClass(e)
            && e.getMessage().replace('.', '/').startsWith("org/hyperledger/besu/"));
  }

  /** A class that could not be found at all, not one whose static initializer failed earlier. */
  private static boolean isMissingClass(final Throwable e) {
    return e instanceof NoClassDefFoundError
        && e.getMessage() != null
        && !e.getMessage().startsWith("Could not initialize class");
  }

  private boolean registerPlugin(final BesuPlugin plugin) {
    try {
      plugin.register(registrationContextFor(plugin));
      LOG.info("Registered plugin of type {}.", pluginId(plugin));
    } catch (final Exception | LinkageError e) {
      final Optional<Throwable> unrecoverable = unrecoverableCause(e);
      if (unrecoverable.isPresent()) {
        throw new RuntimeException(
            "Plugin " + pluginId(plugin) + ": " + unrecoverable.get().getMessage(), e);
      }
      final String failure = failureMessage("registering", pluginId(plugin), e);
      if (config.isContinueOnPluginError()) {
        LOG.error("{}, start and stop will not be called.", failure, e);
        // recorded when its options were defined; a plugin that is not registered is not listed
        pluginVersions.remove(pluginId(plugin));
        // nothing can have looked up what it published yet, so retract it: a later plugin may
        // publish the same type, and no one is handed a service of a plugin that never starts
        pluginServices.values().removeIf(entry -> entry.provider() == plugin);
      } else {
        throw new RuntimeException(failure, e);
      }
      return false;
    }
    return true;
  }

  /** Calls {@link BesuPlugin#start(StartContext)} on every registered plugin. */
  public void startPlugins() {
    checkState(
        state == Lifecycle.REGISTERED,
        "BesuContext should be in state %s but it was in %s",
        Lifecycle.REGISTERED,
        state);
    state = Lifecycle.BEFORE_MAIN_LOOP_STARTED;
    final Iterator<BesuPlugin> pluginsIterator = registeredPlugins.iterator();

    while (pluginsIterator.hasNext()) {
      final BesuPlugin plugin = pluginsIterator.next();

      try {
        plugin.start(startContextFor(plugin));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Started plugin of type {}.", pluginId(plugin));
        }
      } catch (final Exception | LinkageError e) {
        final Optional<Throwable> unrecoverable = unrecoverableCause(e);
        if (unrecoverable.isPresent()) {
          throw new RuntimeException(
              "Plugin " + pluginId(plugin) + ": " + unrecoverable.get().getMessage(), e);
        }
        final String failure = failureMessage("starting", pluginId(plugin), e);
        if (config.isContinueOnPluginError()) {
          LOG.error("{}, stop will not be called.", failure, e);
          pluginsIterator.remove();
        } else {
          throw new RuntimeException(failure, e);
        }
      }
    }

    LOG.debug("Plugin startup complete.");
    state = Lifecycle.BEFORE_MAIN_LOOP_FINISHED;
  }

  /**
   * Calls {@link BesuPlugin#afterMainLoop(RunningContext)} on every started plugin, once the main
   * loop is running.
   */
  public void afterMainLoop() {
    checkState(
        state == Lifecycle.BEFORE_MAIN_LOOP_FINISHED,
        "BesuContext should be in state %s but it was in %s",
        Lifecycle.BEFORE_MAIN_LOOP_FINISHED,
        state);
    final Iterator<BesuPlugin> pluginsIterator = registeredPlugins.iterator();

    while (pluginsIterator.hasNext()) {
      final BesuPlugin plugin = pluginsIterator.next();
      try {
        plugin.afterMainLoop(runningContextFor(plugin));
      } catch (final Exception | LinkageError e) {
        final Optional<Throwable> unrecoverable = unrecoverableCause(e);
        if (unrecoverable.isPresent()) {
          throw new RuntimeException(
              "Plugin " + pluginId(plugin) + ": " + unrecoverable.get().getMessage(), e);
        }
        final String failure = failureMessage("calling `afterMainLoop` on", pluginId(plugin), e);
        if (config.isContinueOnPluginError()) {
          LOG.error("{}, stop will not be called.", failure, e);
          pluginsIterator.remove();
        } else {
          throw new RuntimeException(failure, e);
        }
      }
    }
  }

  /** Stop plugins. */
  public void stopPlugins() {
    checkState(
        state == Lifecycle.BEFORE_MAIN_LOOP_FINISHED,
        "BesuContext should be in state %s but it was in %s",
        Lifecycle.BEFORE_MAIN_LOOP_FINISHED,
        state);
    state = Lifecycle.STOPPING;

    for (final BesuPlugin plugin : registeredPlugins) {
      try {
        plugin.stop();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Stopped plugin of type {}.", pluginId(plugin));
        }
      } catch (final Exception e) {
        LOG.error("Error stopping plugin of type " + pluginId(plugin), e);
      }
    }

    // Close the plugin classloader to release file handles
    if (pluginClassLoader != null) {
      try {
        pluginClassLoader.close();
        LOG.debug("Closed plugin classloader");
      } catch (final IOException e) {
        LOG.debug("Error closing plugin classloader", e);
      }
    }

    LOG.debug("Plugin shutdown complete.");
    state = Lifecycle.STOPPED;
  }

  private static URL pathToURIOrNull(final Path p) {
    try {
      return p.toUri().toURL();
    } catch (final MalformedURLException e) {
      return null;
    }
  }

  /**
   * Puts the jars of the plugins directory, in file name order, on the plugin class loader and
   * discovers the plugins they declare, without instantiating any. A plugins directory that exists
   * but cannot be read, a jar that cannot be opened, or a plugin declaration that cannot be loaded,
   * fails startup; a missing directory means no plugins, a path that is not a directory fails
   * startup.
   *
   * @return the detected plugins, in load order
   */
  @VisibleForTesting
  List<ServiceLoader.Provider<BesuPlugin>> detectPlugins() {
    final Path pluginsDir = config.getPluginsDir().toAbsolutePath();
    if (Files.notExists(pluginsDir)) {
      LOG.info("Plugins directory {} does not exist, no plugins will be loaded", pluginsDir);
      return List.of();
    }
    if (!Files.isDirectory(pluginsDir)) {
      throw new IllegalStateException("Plugins directory " + pluginsDir + " is not a directory");
    }
    LOG.debug("Searching for plugins in {}", pluginsDir);

    final List<Path> jars;
    try (final Stream<Path> pluginFiles = Files.list(pluginsDir)) {
      jars =
          pluginFiles
              .filter(p -> p.getFileName().toString().endsWith(".jar"))
              .sorted(Comparator.comparing(p -> p.getFileName().toString()))
              .toList();
    } catch (final IOException e) {
      throw new IllegalStateException(
          "Cannot read the plugins directory " + pluginsDir + " (" + e + ")", e);
    }
    for (final Path jar : jars) {
      // the class loader opens a jar lazily and silently skips one it cannot open
      try (final JarFile ignored = new JarFile(jar.toFile())) {
        LOG.debug("Found plugin jar {}", jar);
      } catch (final IOException e) {
        throw new IllegalStateException("Cannot open the plugin jar " + jar + " (" + e + ")", e);
      }
    }
    final URL[] pluginJarURLs =
        jars.stream().map(BesuPluginContextImpl::pathToURIOrNull).toArray(URL[]::new);
    // The URLClassLoader must remain open for the entire application lifecycle
    // as plugins may load classes lazily during their operation. The classloader
    // will be closed in stopPlugins() during shutdown.
    this.pluginClassLoader = new URLClassLoader(pluginJarURLs, this.getClass().getClassLoader());

    final List<ServiceLoader.Provider<BesuPlugin>> detected;
    try {
      detected = ServiceLoader.load(BesuPlugin.class, this.pluginClassLoader).stream().toList();
    } catch (final ServiceConfigurationError e) {
      throw new IllegalStateException(
          "Cannot load a plugin declared by a jar in " + pluginsDir + " (" + e.getMessage() + ")",
          e);
    }
    try {
      PluginVerifier.verify(
          config.getPluginsVerificationMode(),
          this.pluginClassLoader,
          detected.stream().map(ServiceLoader.Provider::type).toList());
    } catch (final IOException e) {
      throw new IllegalStateException(
          "Cannot verify the plugins in " + pluginsDir + " (" + e + ")", e);
    }
    return detected;
  }

  @Override
  public Map<String, String> getPluginVersions() {
    return Collections.unmodifiableMap(pluginVersions);
  }

  /**
   * Gets plugins.
   *
   * @return the plugins
   */
  @VisibleForTesting
  List<BesuPlugin> getRegisteredPlugins() {
    return Collections.unmodifiableList(registeredPlugins);
  }

  /**
   * Gets the registered plugins by id.
   *
   * @return the plugins by id
   */
  public Map<String, BesuPlugin> getPluginsById() {
    return registeredPlugins.stream()
        .collect(Collectors.toMap(BesuPluginContextImpl::pluginId, plugin -> plugin));
  }

  /**
   * Generates a summary log of plugin registration: the registered plugins, the selected plugins
   * that did not register and the detected plugins the configuration did not select, each by id,
   * with the version of those that were instantiated.
   *
   * @return A list of strings, each representing a line in the summary log.
   */
  public List<String> getPluginsSummaryLog() {
    final List<String> summary = new ArrayList<>();
    summary.add("Plugin Registration Summary:");

    if (registeredPlugins.isEmpty()) {
      summary.add("No plugins have been registered.");
    } else {
      summary.add("Registered Plugins:");
      registeredPlugins.forEach(plugin -> summary.add(" - " + describe(pluginId(plugin))));
    }

    final Set<String> registeredIds =
        registeredPlugins.stream().map(BesuPluginContextImpl::pluginId).collect(Collectors.toSet());
    final List<String> notRegistered =
        selectedPluginIds.stream().filter(id -> !registeredIds.contains(id)).toList();
    if (!notRegistered.isEmpty()) {
      summary.add("Detected but not registered:");
      notRegistered.forEach(id -> summary.add(" - " + describe(id)));
    }

    final List<String> notSelected =
        detectedPluginIds.stream().filter(id -> !selectedPluginIds.contains(id)).toList();
    if (!notSelected.isEmpty()) {
      summary.add("Detected but not selected:");
      notSelected.forEach(id -> summary.add(" - " + id));
    }
    summary.add(
        String.format(
            "TOTAL = %d of %d plugins successfully registered.",
            registeredPlugins.size(), detectedPluginIds.size()));

    return summary;
  }

  /** The id of a plugin, followed by its version when the plugin was instantiated. */
  private String describe(final String pluginId) {
    final BesuPlugin plugin = instantiatedPlugins.get(pluginId);
    return plugin == null ? pluginId : pluginId + " (" + plugin.getVersion() + ")";
  }

  /**
   * Rewinds to the options-defined state so the Ephemery restart can register the same plugin
   * instances again, without re-parsing the command line. The services plugins published in the
   * previous cycle are dropped, so the second registration pass starts from the same empty set the
   * first one did.
   */
  public void resetState() {
    pluginServices.clear();
    state = Lifecycle.OPTIONS_DEFINED;
  }
}
