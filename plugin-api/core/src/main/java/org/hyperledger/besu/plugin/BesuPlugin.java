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
package org.hyperledger.besu.plugin;

import org.hyperledger.besu.plugin.services.PicoCLIOptions;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Base interface for Besu plugins.
 *
 * <p>Plugins are discovered and loaded using {@link java.util.ServiceLoader} from jar files within
 * Besu's plugin directory. See the {@link java.util.ServiceLoader} documentation for how to
 * register plugins.
 */
public interface BesuPlugin {

  /**
   * Returns the name of the plugin. This name is used to trigger specific actions on individual
   * plugins.
   *
   * @return the name of the plugin.
   */
  default String getName() {
    return this.getClass().getName();
  }

  /**
   * Called before the command line is parsed, so that the plugin can declare its CLI options
   * through {@link PicoCLIOptions#addPicoCLIOptions(String, Object)}.
   *
   * <p>The command line is parsed once, after every plugin has returned from this method; options
   * added later fail Besu startup. No Besu service is available here and the plugin must not do any
   * other work. Unlike {@link #register(RegistrationContext)}, this method also runs for {@code
   * --help}, {@code --version} and {@code --print-paths-and-exit}.
   *
   * @param options the registry to declare the plugin's CLI options with
   */
  default void defineOptions(final PicoCLIOptions options) {}

  /**
   * Called once the command line has been parsed and before the node is built. The option fields
   * declared in {@link #defineOptions(PicoCLIOptions)} hold their configured values, so the plugin
   * can register everything whose shape depends on its configuration (storage factories, security
   * modules, RPC endpoints, validators, permissioning providers, metric categories) and validate
   * its configuration: throwing from this method rejects the configuration before the database is
   * touched, unless the operator chose to continue on plugin errors.
   *
   * <p>This is the only phase in which registrations count: Besu reads them once, after the last
   * plugin has returned from this method. The context only offers the services that work here
   * ({@link RegistrationService} types) and is valid only while this method runs; a {@link
   * RegistrationService} kept in a field and used later rejects the call with a {@link
   * RegistrationClosedException}. Services that work in later phases are obtained from the context
   * of that phase, so there is nothing to keep from this one.
   *
   * <p>The configuration views provide the data path, the storage path, the data storage
   * configuration, the RPC HTTP host, port and timeout and the min gas price here. On Ephemery the
   * per-cycle data subdirectory is chosen when the node is built, so the final data path is only
   * visible from {@link #start(StartContext)}.
   *
   * @param context the registration-phase context
   */
  void register(RegistrationContext context);

  /**
   * Called once Besu has built the node and started its external services, before the main loop is
   * up. The plugin should begin operation: obtain the services it uses, subscribe to events and
   * start any background threads it requires.
   *
   * <p>The context offers every {@link StartService}, and those services keep working for the rest
   * of the node's life, so the context and the services obtained from it may be kept and used from
   * callbacks and from {@link #afterMainLoop(RunningContext)}.
   *
   * @param context the start-phase context
   */
  void start(StartContext context);

  /**
   * Called once the main loop is running: the peer-to-peer network, the synchronizer and the mining
   * coordinator have started. This is the first point at which the {@link RunningService} types
   * ({@code P2PService}, {@code SynchronizationService}, {@code MiningService}) return meaningful
   * answers, so work that needs them belongs here rather than in {@link #start(StartContext)}.
   *
   * @param context the running-phase context, whose lookup also accepts every {@link StartService}
   */
  default void afterMainLoop(final RunningContext context) {}

  /**
   * Called when the plugin is being reloaded. This method will be called through a dedicated JSON
   * RPC endpoint. If not overridden this method does nothing for convenience. The plugin should
   * only implement this method if it supports dynamic reloading.
   *
   * <p>The plugin should reload its configuration dynamically or do nothing if not applicable.
   *
   * @return a {@link CompletableFuture}
   */
  default CompletableFuture<Void> reloadConfiguration() {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Called when the plugin is being stopped. This method will be called as part of Besu shutting
   * down but may also be called at other times to disable the plugin.
   *
   * <p>The plugin should remove any registered listeners and stop any background threads it
   * started.
   */
  void stop();

  /**
   * Retrieves the version information of the plugin. It constructs a version string using the
   * implementation title and version from the package information. If either the title or version
   * is not available, it defaults to "Unknown Implementation Title" and "Unknown Version",
   * respectively.
   *
   * @return A string representing the plugin's version information, formatted as "Title/vVersion".
   */
  default String getVersion() {
    Package pluginPackage = this.getClass().getPackage();
    String implTitle =
        Optional.ofNullable(pluginPackage.getImplementationTitle())
            .filter(title -> !title.isBlank())
            .orElse("<Unknown Implementation Title>");
    String implVersion =
        Optional.ofNullable(pluginPackage.getImplementationVersion())
            .filter(version -> !version.isBlank())
            .orElse("<Unknown Version>");
    return implTitle + "/" + implVersion;
  }
}
