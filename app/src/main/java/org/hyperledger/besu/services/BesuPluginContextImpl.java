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
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.zip.ZipEntry;

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

  /** A plugin declared by a jar of the plugins directory whose class could not be loaded. */
  @VisibleForTesting
  record UnloadablePlugin(String id, Path jar, Throwable cause) {}

  /** What discovery found: the plugins whose class loaded and the declared ones that did not. */
  @VisibleForTesting
  record DetectedPlugins(
      List<ServiceLoader.Provider<BesuPlugin>> loadable, List<UnloadablePlugin> unloadable) {}

  private static final String SERVICE_FILE = "META-INF/services/" + BesuPlugin.class.getName();

  private volatile Lifecycle state = Lifecycle.UNINITIALIZED;
  private final Map<Class<?>, BesuService> besuServices = new ConcurrentHashMap<>();
  private final Map<Class<?>, ServiceWithProvider> pluginServices = new ConcurrentHashMap<>();

  /** The ids of every plugin found in the plugins directory, in load order, unloadable last. */
  private List<String> detectedPluginIds = List.of();

  /**
   * The ids of the detected plugins that the configuration selects, in load order, unloadable last.
   */
  private List<String> selectedPluginIds = List.of();

  /** The ids of the detected plugins whose class could not be loaded. */
  private Set<String> unloadablePluginIds = Set.of();

  /** The selected plugins that were instantiated, by id, in load order. */
  private final Map<String, BesuPlugin> instantiatedPlugins = new LinkedHashMap<>();

  /** Plugins supplied by Besu itself, ahead of the discovered ones in every phase. */
  private final List<BesuPlugin> builtInPlugins = new ArrayList<>();

  /** Plugins that completed {@code defineOptions()}, built-ins first, then in load order. */
  private final List<BesuPlugin> loadedPlugins = new ArrayList<>();

  private final List<BesuPlugin> registeredPlugins = new ArrayList<>();

  /** Plugins whose {@code start()} returned in the current cycle: the ones a stop reaches. */
  private final List<BesuPlugin> startedPlugins = new ArrayList<>();

  private final Map<String, String> pluginVersions = new LinkedHashMap<>();
  private PluginConfiguration config;
  private URLClassLoader pluginClassLoader;

  /**
   * The id of the plugin whose {@code register()} is running; null outside a plugin's register().
   */
  private volatile String currentRegistrant;

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
   * The id of the plugin whose {@code register()} is running. The registration-tier services record
   * it with each named entry a plugin registers, so a second registration of the same name can be
   * told apart as the same plugin registering again or another plugin colliding with it.
   *
   * @return the registering plugin's id; empty when Besu itself is registering, outside any
   *     plugin's register()
   */
  public Optional<String> currentRegistrant() {
    return Optional.ofNullable(currentRegistrant);
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
    currentRegistrant = pluginName;
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
   * Adds a plugin that ships inside Besu. Built-in plugins go through the same phases as the
   * discovered plugins and ahead of them in every phase, so a discovered plugin finds what they
   * registered. They are not subject to the plugin selection options or to the verifier, and a
   * built-in failure fails startup regardless of {@code --plugin-continue-on-error}.
   *
   * @param plugin the plugin, added once, before the plugins define their options
   */
  public void addBuiltInPlugin(final BesuPlugin plugin) {
    checkState(
        state == Lifecycle.UNINITIALIZED || state == Lifecycle.INITIALIZED,
        "Built-in plugins must be added before the plugins define their options, not in state %s",
        state);
    checkArgument(
        !builtInPlugins.contains(plugin), "Built-in plugin %s is already added", pluginId(plugin));
    builtInPlugins.add(plugin);
  }

  private boolean isBuiltIn(final BesuPlugin plugin) {
    return builtInPlugins.contains(plugin);
  }

  /**
   * Whether a lifecycle failure of the given plugin is logged and the plugin dropped rather than
   * failing startup: only when the operator chose to continue on plugin errors, and never for a
   * built-in plugin.
   */
  private boolean continuesOnFailureOf(final BesuPlugin plugin) {
    return config.isContinueOnPluginError() && !isBuiltIn(plugin);
  }

  /**
   * The failure message of a fatal failure; when the continue-on-error flag is set but does not
   * cover the plugin, it says why.
   */
  private String fatalFailure(final String failure, final BesuPlugin plugin) {
    return config.isContinueOnPluginError() && isBuiltIn(plugin)
        ? failure + "; a built-in plugin failure is fatal regardless of --plugin-continue-on-error"
        : failure;
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
   * then calls {@link BesuPlugin#defineOptions(PicoCLIOptions)} on each, the built-in plugins
   * first. Runs before the command line is parsed.
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

    final List<BesuPlugin> plugins = new ArrayList<>(builtInPlugins);
    plugins.addAll(loadPlugins());
    for (final BesuPlugin plugin : plugins) {
      try {
        plugin.defineOptions(picoCLIOptions);
        pluginVersions.put(pluginId(plugin), plugin.getVersion());
        loadedPlugins.add(plugin);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Defined options of plugin of type {}.", pluginId(plugin));
        }
      } catch (final Exception | LinkageError e) {
        final String failure = failureMessage("defining options of", pluginId(plugin), e);
        if (continuesOnFailureOf(plugin)) {
          LOG.error("{}, register, start and stop will not be called.", failure, e);
        } else {
          throw new RuntimeException(fatalFailure(failure, plugin), e);
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
    final DetectedPlugins detected = detectPlugins();
    // every detected plugin by id, with its simple class name, in load order, unloadable last
    final Map<String, String> simpleNamesById = new LinkedHashMap<>();
    detected
        .loadable()
        .forEach(
            provider ->
                simpleNamesById.put(pluginId(provider.type()), provider.type().getSimpleName()));
    detected
        .unloadable()
        .forEach(plugin -> simpleNamesById.put(plugin.id(), simpleClassName(plugin.id())));
    detectedPluginIds = List.copyOf(simpleNamesById.keySet());
    unloadablePluginIds =
        detected.unloadable().stream().map(UnloadablePlugin::id).collect(Collectors.toSet());
    final Set<String> selectedIds =
        config.getRequestedPlugins().isEmpty()
            ? simpleNamesById.keySet()
            : selectRequestedPlugins(config.getRequestedPlugins(), simpleNamesById);
    selectedPluginIds = detectedPluginIds.stream().filter(selectedIds::contains).toList();
    failOnSelectedUnloadablePlugins(detected.unloadable(), selectedIds);
    return instantiate(
        detected.loadable().stream()
            .filter(provider -> selectedIds.contains(pluginId(provider.type())))
            .toList());
  }

  /** The simple name of a class that could not be loaded, taken from its fully qualified name. */
  private static String simpleClassName(final String className) {
    final String withoutPackage = className.substring(className.lastIndexOf('.') + 1);
    return withoutPackage.substring(withoutPackage.lastIndexOf('$') + 1);
  }

  /**
   * Applies the plugin failure policy to the detected plugins whose class could not be loaded. One
   * that the configuration selects fails startup, or is skipped with {@code
   * --plugin-continue-on-error}; one that it leaves out is ignored, as a plugin that is never
   * instantiated is.
   */
  private void failOnSelectedUnloadablePlugins(
      final List<UnloadablePlugin> unloadable, final Set<String> selectedIds) {
    for (final UnloadablePlugin plugin : unloadable) {
      final String failure =
          failureMessage(
              "loading",
              plugin.id() + " (declared by " + plugin.jar().getFileName() + ")",
              plugin.cause());
      if (!selectedIds.contains(plugin.id())) {
        LOG.debug("{}, it is not selected and is ignored.", failure);
      } else if (config.isContinueOnPluginError()) {
        LOG.error(
            "{}, defineOptions, register, start and stop will not be called.",
            failure,
            plugin.cause());
      } else {
        throw new RuntimeException(failure, plugin.cause());
      }
    }
  }

  /**
   * Resolves each requested plugin to one of the detected plugins: by id first, otherwise by simple
   * class name when exactly one detected plugin has it.
   *
   * @param simpleNamesById the simple class name of every detected plugin by id, in load order,
   *     unloadable last
   * @return the ids of the selected plugins
   * @throws IllegalArgumentException if a name is no plugin's id and is the simple class name of
   *     several detected plugins
   * @throws NoSuchElementException if a requested plugin was not detected
   */
  private static Set<String> selectRequestedPlugins(
      final List<String> requestedPlugins, final Map<String, String> simpleNamesById) {
    final Set<String> selectedIds = new HashSet<>();
    final List<String> missing = new ArrayList<>();
    for (final String requested : requestedPlugins) {
      if (simpleNamesById.containsKey(requested)) {
        selectedIds.add(requested);
        continue;
      }
      final List<String> matchingIds =
          simpleNamesById.entrySet().stream()
              .filter(entry -> requested.equals(entry.getValue()))
              .map(Map.Entry::getKey)
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
    return selectedIds;
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
    currentRegistrant = null;
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
      if (continuesOnFailureOf(plugin)) {
        LOG.error("{}, start and stop will not be called.", failure, e);
        // recorded when its options were defined; a plugin that is not registered is not listed
        pluginVersions.remove(pluginId(plugin));
        // nothing can have looked up what it published yet, so retract it: a later plugin may
        // publish the same type, and no one is handed a service of a plugin that never starts
        pluginServices.values().removeIf(entry -> entry.provider() == plugin);
      } else {
        throw new RuntimeException(fatalFailure(failure, plugin), e);
      }
      return false;
    } finally {
      currentRegistrant = null;
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
    startedPlugins.clear();
    final Iterator<BesuPlugin> pluginsIterator = registeredPlugins.iterator();

    while (pluginsIterator.hasNext()) {
      final BesuPlugin plugin = pluginsIterator.next();

      try {
        plugin.start(startContextFor(plugin));
        startedPlugins.add(plugin);
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
        if (continuesOnFailureOf(plugin)) {
          LOG.error("{}, stop will not be called.", failure, e);
          pluginsIterator.remove();
        } else {
          throw new RuntimeException(fatalFailure(failure, plugin), e);
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
        if (continuesOnFailureOf(plugin)) {
          LOG.error("{}, stop will not be called.", failure, e);
          pluginsIterator.remove();
          startedPlugins.remove(plugin);
        } else {
          throw new RuntimeException(fatalFailure(failure, plugin), e);
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
    stopStartedPlugins();

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

  /**
   * Stops the plugins the current cycle started, for an Ephemery restart, before the node's
   * services stop, in the order of a shutdown. Accepted after registration whether the start phase
   * ran to the end, failed part way or never ran, so a cycle that failed to start does not block
   * the next one: only the plugins whose {@code start()} returned are stopped. Unlike {@link
   * #stopPlugins()} this keeps the plugin class loader open and returns to the registered state,
   * since the same plugins register and run again.
   *
   * @throws IllegalStateException if the context is not in the REGISTERED, the
   *     BEFORE_MAIN_LOOP_STARTED or the BEFORE_MAIN_LOOP_FINISHED state.
   */
  public void stopPluginsForRestart() {
    checkState(
        state == Lifecycle.REGISTERED
            || state == Lifecycle.BEFORE_MAIN_LOOP_STARTED
            || state == Lifecycle.BEFORE_MAIN_LOOP_FINISHED,
        "BesuContext should be in state %s, %s or %s but it was in %s",
        Lifecycle.REGISTERED,
        Lifecycle.BEFORE_MAIN_LOOP_STARTED,
        Lifecycle.BEFORE_MAIN_LOOP_FINISHED,
        state);
    if (state == Lifecycle.BEFORE_MAIN_LOOP_STARTED) {
      LOG.warn(
          "The previous cycle failed while starting its plugins; stopping the {} it had started",
          startedPlugins.size());
    }
    stopStartedPlugins();
    state = Lifecycle.REGISTERED;
  }

  private void stopStartedPlugins() {
    for (final BesuPlugin plugin : startedPlugins) {
      try {
        plugin.stop();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Stopped plugin of type {}.", pluginId(plugin));
        }
      } catch (final Exception e) {
        LOG.error("Error stopping plugin of type " + pluginId(plugin), e);
      }
    }
    startedPlugins.clear();
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
   * but cannot be accessed or read, a path that is not a directory, or a jar that cannot be opened,
   * fails startup; a missing directory means no plugins. A declared plugin whose class cannot be
   * loaded, such as one with a missing dependency, is returned as unloadable for the caller to
   * apply the plugin failure policy to.
   *
   * @return the detected plugins, in load order
   */
  @VisibleForTesting
  DetectedPlugins detectPlugins() {
    final Path pluginsDir = config.getPluginsDir().toAbsolutePath();
    if (Files.notExists(pluginsDir)) {
      LOG.info("Plugins directory {} does not exist, no plugins will be loaded", pluginsDir);
      return new DetectedPlugins(List.of(), List.of());
    }
    if (!Files.exists(pluginsDir)) {
      // neither exists nor notExists holds when a parent directory cannot be traversed
      throw new IllegalStateException("Cannot access the plugins directory " + pluginsDir);
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
              // skip hidden files: an archive made on macOS, unpacked on another system, leaves
              // a ._<jar name> metadata file beside each jar, which is not a jar
              .filter(p -> !p.getFileName().toString().startsWith("."))
              .sorted(Comparator.comparing(p -> p.getFileName().toString()))
              .toList();
    } catch (final IOException e) {
      throw new IllegalStateException(
          "Cannot read the plugins directory " + pluginsDir + " (" + e + ")", e);
    }
    // the plugin classes each jar declares, by name; the first jar to declare a name provides it
    final Map<String, Path> declaredPlugins = new LinkedHashMap<>();
    for (final Path jar : jars) {
      // the class loader opens a jar lazily and silently skips one it cannot open
      try (final JarFile jarFile = new JarFile(jar.toFile())) {
        LOG.debug("Found plugin jar {}", jar);
        declaredPluginsOf(jarFile).forEach(name -> declaredPlugins.putIfAbsent(name, jar));
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

    // One provider at a time: a class that cannot be loaded makes the service loader throw, a
    // ServiceConfigurationError or, when a supertype is missing, a raw LinkageError, and the
    // loader carries on with the next declaration when asked again. A failure that no
    // declaration accounts for ends the loop.
    final List<ServiceLoader.Provider<BesuPlugin>> loadable = new ArrayList<>();
    final List<Throwable> failures = new ArrayList<>();
    final Iterator<ServiceLoader.Provider<BesuPlugin>> providers =
        ServiceLoader.load(BesuPlugin.class, this.pluginClassLoader).stream().iterator();
    while (failures.size() <= declaredPlugins.size()) {
      try {
        if (!providers.hasNext()) {
          break;
        }
        loadable.add(providers.next());
      } catch (final ServiceConfigurationError | LinkageError e) {
        failures.add(e);
      }
    }

    // find out why the loader skipped each declared plugin; one whose class loads fine means a
    // malformed service file
    final Set<String> loadableIds =
        loadable.stream().map(provider -> pluginId(provider.type())).collect(Collectors.toSet());
    final List<UnloadablePlugin> unloadable = new ArrayList<>();
    declaredPlugins.forEach(
        (name, jar) -> {
          if (!loadableIds.contains(name)) {
            final Throwable cause =
                loadFailure(name).orElseThrow(() -> notDiscovered(name, jar, failures));
            unloadable.add(new UnloadablePlugin(name, jar, cause));
          }
        });
    if (failures.size() > unloadable.size()) {
      // not a plugin of the plugins directory, so there is no plugin to apply the policy to
      throw new IllegalStateException(
          "Cannot load a plugin that no jar in "
              + pluginsDir
              + " declares, the service loader reported: "
              + failures.stream().map(Throwable::toString).collect(Collectors.joining("; ")),
          failures.getFirst());
    }

    try {
      PluginVerifier.verify(
          config.getPluginsVerificationMode(),
          this.pluginClassLoader,
          loadable.stream().map(ServiceLoader.Provider::type).toList());
    } catch (final IOException e) {
      throw new IllegalStateException(
          "Cannot verify the plugins in " + pluginsDir + " (" + e + ")", e);
    }
    return new DetectedPlugins(loadable, unloadable);
  }

  /**
   * The failure for a declared plugin that is fine but that the service loader did not hand over,
   * as happens to every plugin of a service file with a malformed line; what the loader reported
   * says why.
   */
  private static IllegalStateException notDiscovered(
      final String pluginClassName, final Path jar, final List<Throwable> loaderFailures) {
    final String reported =
        loaderFailures.stream().map(Throwable::toString).collect(Collectors.joining("; "));
    return new IllegalStateException(
        "Plugin "
            + pluginClassName
            + " declared by "
            + jar
            + " was not discovered by the service loader"
            + (reported.isEmpty() ? "" : ", which reported: " + reported),
        loaderFailures.isEmpty() ? null : loaderFailures.getFirst());
  }

  /** The plugin class names the service file of a jar declares, in file order. */
  private static List<String> declaredPluginsOf(final JarFile jar) throws IOException {
    final ZipEntry serviceFile = jar.getEntry(SERVICE_FILE);
    if (serviceFile == null) {
      return List.of();
    }
    try (final InputStream in = jar.getInputStream(serviceFile)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8)
          .lines()
          .map(line -> line.contains("#") ? line.substring(0, line.indexOf('#')) : line)
          .map(String::trim)
          .filter(line -> !line.isEmpty())
          .toList();
    }
  }

  /**
   * Why the service loader could not hand over a declared plugin: its class or a supertype is
   * missing, it does not implement {@link BesuPlugin}, or it has no public no-argument constructor.
   *
   * @return the failure, empty if the plugin can be loaded
   */
  private Optional<Throwable> loadFailure(final String pluginClassName) {
    try {
      final Class<?> pluginType = Class.forName(pluginClassName, false, this.pluginClassLoader);
      if (!BesuPlugin.class.isAssignableFrom(pluginType)) {
        return Optional.of(
            new ClassCastException(
                pluginClassName + " does not implement " + BesuPlugin.class.getName()));
      }
      final boolean hasPublicNoArgConstructor =
          Arrays.stream(pluginType.getConstructors())
              .anyMatch(constructor -> constructor.getParameterCount() == 0);
      return hasPublicNoArgConstructor
          ? Optional.empty()
          : Optional.of(new NoSuchMethodException(pluginClassName + ".<init>()"));
    } catch (final ClassNotFoundException | LinkageError e) {
      return Optional.of(e);
    }
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
   * with the version of those that were instantiated and a note on those whose class could not be
   * loaded.
   *
   * @return A list of strings, each representing a line in the summary log.
   */
  public List<String> getPluginsSummaryLog() {
    final List<String> summary = new ArrayList<>();
    summary.add("Plugin Registration Summary:");

    final List<BesuPlugin> registeredBuiltIns =
        registeredPlugins.stream().filter(this::isBuiltIn).toList();
    if (!registeredBuiltIns.isEmpty()) {
      summary.add("Built-in plugins:");
      registeredBuiltIns.forEach(plugin -> summary.add(" - " + describe(plugin)));
    }

    // the plugins from the plugins directory, counted against what the directory held
    final List<BesuPlugin> registeredExternalPlugins =
        registeredPlugins.stream().filter(plugin -> !isBuiltIn(plugin)).toList();
    if (registeredExternalPlugins.isEmpty()) {
      summary.add("No external plugins have been registered.");
    } else {
      summary.add("Registered Plugins:");
      registeredExternalPlugins.forEach(plugin -> summary.add(" - " + describe(plugin)));
    }

    final Set<String> registeredIds =
        registeredExternalPlugins.stream()
            .map(BesuPluginContextImpl::pluginId)
            .collect(Collectors.toSet());
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
      notSelected.forEach(id -> summary.add(" - " + describe(id)));
    }
    summary.add(
        String.format(
            "TOTAL = %d of %d plugins successfully registered.",
            registeredExternalPlugins.size(), detectedPluginIds.size()));

    return summary;
  }

  /**
   * The id of a plugin, followed by its version when the plugin was instantiated, or by a note when
   * its class could not be loaded.
   */
  private String describe(final String pluginId) {
    if (unloadablePluginIds.contains(pluginId)) {
      return pluginId + " (could not be loaded)";
    }
    final BesuPlugin plugin = instantiatedPlugins.get(pluginId);
    return plugin == null ? pluginId : pluginId + " (" + plugin.getVersion() + ")";
  }

  /** The id of an instantiated plugin, followed by its version. */
  private static String describe(final BesuPlugin plugin) {
    return pluginId(plugin) + " (" + plugin.getVersion() + ")";
  }

  /**
   * The configuration overview lines of every registered plugin, by plugin id, in registration
   * order. Plugins with nothing to show are left out.
   *
   * @return the lines to print under each plugin's id
   */
  public Map<String, List<String>> getConfigurationOverviewLines() {
    final Map<String, List<String>> overview = new LinkedHashMap<>();
    for (final BesuPlugin plugin : registeredPlugins) {
      try {
        final List<String> lines = plugin.configurationOverviewLines();
        if (!lines.isEmpty()) {
          overview.put(pluginId(plugin), List.copyOf(lines));
        }
      } catch (final Exception | LinkageError e) {
        final String failure =
            failureMessage("reading the configuration overview of", pluginId(plugin), e);
        if (continuesOnFailureOf(plugin)) {
          LOG.error("{}, its lines are left out.", failure, e);
        } else {
          throw new RuntimeException(fatalFailure(failure, plugin), e);
        }
      }
    }
    return overview;
  }

  /**
   * Rewinds to the options-defined state, once {@link #stopPluginsForRestart()} has stopped the
   * previous cycle, so the Ephemery restart can register the same plugin instances again without
   * re-parsing the command line. The services plugins published in the previous cycle are dropped,
   * so the second registration pass starts from the same empty set the first one did.
   *
   * @throws IllegalStateException if the context is not in the REGISTERED state, that is if the
   *     plugins of the previous cycle were not stopped first.
   */
  public void resetState() {
    checkState(
        state == Lifecycle.REGISTERED,
        "BesuContext should be in state %s but it was in %s; stop the plugins of the previous cycle"
            + " first",
        Lifecycle.REGISTERED,
        state);
    pluginServices.clear();
    state = Lifecycle.OPTIONS_DEFINED;
  }
}
