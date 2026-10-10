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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
  private record PluginProvidedService(String owner, BesuService service) {}

  private volatile Lifecycle state = Lifecycle.UNINITIALIZED;
  private final Map<Class<?>, BesuService> besuServices = new ConcurrentHashMap<>();
  private final Map<Class<?>, PluginProvidedService> pluginServices = new ConcurrentHashMap<>();

  private List<BesuPlugin> detectedPlugins = new ArrayList<>();
  private List<String> requestedPlugins = new ArrayList<>();

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
   * Whether plugin registration is open, that is whether a plugin's {@code register()} is running.
   * The registration-tier services consult this to reject late registrations.
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
        "Plugin-provided services can only be read once registration has closed");
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
    final String pluginName = plugin.getName();
    return new RegistrationContext() {
      @Override
      public <T extends RegistrationService> T getBesuService(final Class<T> type) {
        if (!isRegistering()) {
          throw new IllegalStateException(
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
          throw new UnrecoverablePluginException(
              "registered "
                  + type.getSimpleName()
                  + " after register() returned; plugin-provided services must be registered from"
                  + " BesuPlugin.register()");
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
        final PluginProvidedService previous =
            pluginServices.putIfAbsent(type, new PluginProvidedService(pluginName, service));
        if (previous != null) {
          throw new UnrecoverablePluginException(
              "cannot register "
                  + type.getSimpleName()
                  + ": plugin "
                  + previous.owner()
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
    final String pluginName = plugin.getName();
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
    final String pluginName = plugin.getName();
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
        pluginVersions.put(plugin.getName(), plugin.getVersion());
        loadedPlugins.add(plugin);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Defined options of plugin of type {}.", plugin.getClass().getName());
        }
      } catch (final Exception | LinkageError e) {
        final String failure = failureMessage("defining options of", plugin, e);
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
   * Discovers, verifies and instantiates the plugins selected by the configuration.
   *
   * @return the plugins to define options for, empty when external plugins are disabled
   */
  @VisibleForTesting
  List<BesuPlugin> loadPlugins() {
    if (!config.isExternalPluginsEnabled()) {
      LOG.debug("External plugins are disabled. Skipping plugins loading.");
      return List.of();
    }
    detectedPlugins = detectPlugins(config);
    if (config.getRequestedPlugins().isEmpty()) {
      // If no plugins were specified, load all detected plugins
      return detectedPlugins;
    }
    // Load only the plugins that were explicitly requested and validated
    requestedPlugins = config.getRequestedPlugins();
    return matchAndValidateRequestedPlugins(requestedPlugins, detectedPlugins);
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

  private List<BesuPlugin> matchAndValidateRequestedPlugins(
      final List<String> requestedPluginNames, final List<BesuPlugin> detectedPlugins)
      throws NoSuchElementException {

    // Filter detected plugins to include only those that match the requested names
    List<BesuPlugin> matchingPlugins =
        detectedPlugins.stream()
            .filter(plugin -> requestedPluginNames.contains(plugin.getClass().getSimpleName()))
            .toList();

    // Check if all requested plugins were found among the detected plugins
    if (matchingPlugins.size() != requestedPluginNames.size()) {
      // Find which requested plugins were not matched to throw a detailed exception
      Set<String> matchedPluginNames =
          matchingPlugins.stream()
              .map(plugin -> plugin.getClass().getSimpleName())
              .collect(Collectors.toSet());
      String missingPlugins =
          requestedPluginNames.stream()
              .filter(name -> !matchedPluginNames.contains(name))
              .collect(Collectors.joining(", "));
      throw new NoSuchElementException(
          "The following requested plugins were not found: " + missingPlugins);
    }
    return matchingPlugins;
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
   * Describes the failure of a lifecycle call. A linkage error means the plugin was compiled
   * against a plugin API that this Besu version does not provide, typically an older one, so the
   * call could not even be made; it is reported as such rather than as a bare error.
   */
  private static String failureMessage(
      final String action, final BesuPlugin plugin, final Throwable e) {
    final String pluginType = plugin.getClass().getName();
    if (e instanceof LinkageError) {
      return "Plugin of type "
          + pluginType
          + " is not compatible with the plugin API of this Besu version and must be rebuilt"
          + " against it ("
          + e
          + ")";
    }
    return "Error " + action + " plugin of type " + pluginType;
  }

  private boolean registerPlugin(final BesuPlugin plugin) {
    try {
      plugin.register(registrationContextFor(plugin));
      LOG.info("Registered plugin of type {}.", plugin.getClass().getName());
    } catch (final Exception | LinkageError e) {
      final Optional<Throwable> unrecoverable = unrecoverableCause(e);
      if (unrecoverable.isPresent()) {
        throw new RuntimeException(
            "Plugin " + plugin.getClass().getName() + ": " + unrecoverable.get().getMessage(), e);
      }
      final String failure = failureMessage("registering", plugin, e);
      if (config.isContinueOnPluginError()) {
        LOG.error("{}, start and stop will not be called.", failure, e);
        // recorded when its options were defined; a plugin that is not registered is not listed
        pluginVersions.remove(plugin.getName());
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
          LOG.debug("Started plugin of type {}.", plugin.getClass().getName());
        }
      } catch (final Exception | LinkageError e) {
        final Optional<Throwable> unrecoverable = unrecoverableCause(e);
        if (unrecoverable.isPresent()) {
          throw new RuntimeException(
              "Plugin " + plugin.getClass().getName() + ": " + unrecoverable.get().getMessage(), e);
        }
        final String failure = failureMessage("starting", plugin, e);
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
              "Plugin " + plugin.getClass().getName() + ": " + unrecoverable.get().getMessage(), e);
        }
        final String failure = failureMessage("calling `afterMainLoop` on", plugin, e);
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
          LOG.debug("Stopped plugin of type {}.", plugin.getClass().getName());
        }
      } catch (final Exception e) {
        LOG.error("Error stopping plugin of type " + plugin.getClass().getName(), e);
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

  private List<BesuPlugin> detectPlugins(final PluginConfiguration config) {
    final var pluginsDir = config.getPluginsDir();
    if (pluginsDir != null && pluginsDir.toFile().isDirectory()) {
      LOG.debug("Searching for plugins in {}", pluginsDir.toAbsolutePath());

      try (final Stream<Path> pluginFilesList = Files.list(pluginsDir)) {
        final URL[] pluginJarURLs =
            pluginFilesList
                .filter(p -> p.getFileName().toString().endsWith(".jar"))
                .map(BesuPluginContextImpl::pathToURIOrNull)
                .toArray(URL[]::new);
        // The URLClassLoader must remain open for the entire application lifecycle
        // as plugins may load classes lazily during their operation. The classloader
        // will be closed in stopPlugins() during shutdown.
        this.pluginClassLoader =
            new URLClassLoader(pluginJarURLs, this.getClass().getClassLoader());
        ServiceLoader<BesuPlugin> serviceLoader =
            ServiceLoader.load(BesuPlugin.class, this.pluginClassLoader);
        final var foundPlugins = StreamSupport.stream(serviceLoader.spliterator(), false).toList();
        PluginVerifier.verify(
            config.getPluginsVerificationMode(), this.pluginClassLoader, foundPlugins);

        return foundPlugins;
      } catch (final MalformedURLException e) {
        LOG.error("Error converting files to URLs, could not load plugins", e);
      } catch (final IOException e) {
        LOG.error("Error enumerating plugins, could not load plugins", e);
      }
    } else {
      LOG.debug("Plugin directory does not exist, skipping registration. - {}", pluginsDir);
    }

    return List.of();
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
   * Gets plugins by name.
   *
   * @return the plugins by name
   */
  public Map<String, BesuPlugin> getPluginsByName() {
    return registeredPlugins.stream()
        .collect(Collectors.toMap(plugin -> plugin.getName(), plugin -> plugin));
  }

  /**
   * Generates a summary log of plugin registration. The summary includes registered plugins,
   * detected but not registered (skipped) plugins
   *
   * @return A list of strings, each representing a line in the summary log.
   */
  public List<String> getPluginsSummaryLog() {
    List<String> summary = new ArrayList<>();
    summary.add("Plugin Registration Summary:");

    // Log registered plugins with their names and versions
    if (registeredPlugins.isEmpty()) {
      summary.add("No plugins have been registered.");
    } else {
      summary.add("Registered Plugins:");
      registeredPlugins.forEach(
          plugin ->
              summary.add(
                  String.format(
                      " - %s (%s)", plugin.getClass().getSimpleName(), plugin.getVersion())));
    }

    // Identify and log detected but not registered (skipped) plugins
    List<BesuPlugin> skippedPlugins =
        detectedPlugins.stream().filter(plugin -> !registeredPlugins.contains(plugin)).toList();

    if (!skippedPlugins.isEmpty()) {
      summary.add("Detected but not registered:");
      skippedPlugins.forEach(
          plugin ->
              summary.add(
                  String.format(
                      " - %s (%s)", plugin.getClass().getSimpleName(), plugin.getVersion())));
    }
    summary.add(
        String.format(
            "TOTAL = %d of %d plugins successfully registered.",
            registeredPlugins.size(), detectedPlugins.size()));

    return summary;
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
