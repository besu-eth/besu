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
package org.hyperledger.besu.plugin;

import org.hyperledger.besu.plugin.services.BesuService;

/**
 * The context handed to {@link BesuPlugin#register(RegistrationContext)}.
 *
 * <p>Its lookup only accepts {@link RegistrationService} types, so requesting a service that is not
 * usable during registration does not compile. The context is valid only during registration, which
 * closes once every plugin, including Besu's built-in ones, has returned from {@code register()}.
 * Using it afterwards fails with an {@link IllegalStateException}; thrown from a later lifecycle
 * method, it stops startup even with {@code --plugin-continue-on-error}.
 */
public interface RegistrationContext {

  /**
   * Returns a Besu-provided service that is usable during registration.
   *
   * <p>Besu constructs the registration-phase services itself and adds them before any plugin
   * registers, so every service the plugin API offers for this phase is provided. The marker
   * interfaces themselves are not services, so a lookup of one fails.
   *
   * @param type the service interface
   * @param <T> the service type
   * @return the service
   * @throws IllegalStateException if the service is not provided on this node, or if the context is
   *     used after registration has closed
   */
  <T extends RegistrationService> T getBesuService(Class<T> type);

  /**
   * Publishes a service provided by this plugin, for Besu or for other plugins to consume.
   *
   * <p>Other plugins look the service up through {@link StartContext#getPluginService(Class)} and
   * {@link RunningContext#getPluginService(Class)}. Besu may call it as soon as registration has
   * closed, before any plugin's {@code start()}, so it must be usable once {@code register()}
   * returns. When Besu continues on plugin errors, a plugin that fails in {@code register()} has
   * what it published withdrawn, while one that fails later keeps its services published.
   * Publishing a type that Besu provides, or a type another plugin already published, fails
   * startup.
   *
   * @param type the service interface, which must not be a Besu-provided service
   * @param service the implementation
   * @param <T> the service type
   * @throws IllegalArgumentException if {@code type} is not an interface or {@code service} does
   *     not implement it; the plugin fails registration like any other plugin error
   * @throws IllegalStateException if called after registration has closed, if {@code type} is a
   *     Besu-provided service, or if another plugin already published it; thrown from a lifecycle
   *     method, these stop startup even with {@code --plugin-continue-on-error}
   */
  <T extends BesuService> void registerService(Class<T> type, T service);
}
