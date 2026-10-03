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
package org.hyperledger.besu.plugin;

import org.hyperledger.besu.plugin.services.BesuService;

/**
 * The context handed to {@link BesuPlugin#register(RegistrationContext)}.
 *
 * <p>Its lookup only accepts {@link RegistrationService} types, so requesting a service that is not
 * usable during registration does not compile. The context is valid only while {@code register()}
 * is running: using it afterwards fails with an {@link IllegalStateException}.
 */
public interface RegistrationContext {

  /**
   * Returns a Besu-provided service that is usable during registration.
   *
   * <p>If the call compiles, the service is there: Besu constructs these services itself and adds
   * them before any plugin registers. The only failure is a service that is not provided on this
   * node's configuration, which is documented on the service and fails the plugin's registration.
   *
   * @param type the service interface
   * @param <T> the service type
   * @return the service
   * @throws IllegalStateException if the service is not provided on this node, or if the context is
   *     used after {@code register()} has returned
   */
  <T extends RegistrationService> T getBesuService(Class<T> type);

  /**
   * Publishes a service provided by this plugin, for Besu or for other plugins to consume.
   *
   * <p>Besu reads the published services once, after the last plugin has registered; other plugins
   * look them up through {@link StartContext#getPluginService(Class)} and {@link
   * RunningContext#getPluginService(Class)}. Publishing a type that Besu provides, or a type
   * another plugin already published, fails startup.
   *
   * @param type the service interface, which must not be a Besu-provided service
   * @param service the implementation
   * @param <T> the service type
   */
  <T extends BesuService> void registerService(Class<T> type, T service);
}
