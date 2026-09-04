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
   * Called first, before the command line is parsed, so that the plugin can declare its CLI options
   * through {@link PicoCLIOptions#addPicoCLIOptions(String, Object)}.
   *
   * <p>This is the only place where options can be declared: the command line is parsed exactly
   * once, right after every plugin has returned from this method, and a plugin that adds options
   * later fails Besu startup. No Besu service is available here and the plugin must not perform any
   * other work. This method also runs for {@code --help} and {@code --version}, so that the
   * plugin's options are listed, whereas {@link #register(ServiceManager)} does not.
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
   * touched.
   *
   * <p>The <code>context</code> parameter should be stored in a field in the plugin. This is the
   * only time it will be provided to the plugin and is how the plugin will interact with Besu.
   *
   * <p>Typically the plugin will not begin operation until the {@link #start()} method is called.
   *
   * @param context the context that provides access to Besu services.
   */
  void register(ServiceManager context);

  /**
   * Called once Besu has loaded configuration and has started external services but before the main
   * loop is up. The plugin should begin operation, including registering any event listener with
   * Besu services and starting any background threads the plugin requires.
   */
  void start();

  /** Hook to execute plugin setup code after external services */
  default void afterExternalServicePostMainLoop() {}

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
