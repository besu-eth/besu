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

import java.util.Optional;

/**
 * The context handed to {@link BesuPlugin#afterMainLoop(RunningContext)}.
 *
 * <p>Its lookup accepts {@link RunningService} types, which includes every {@link StartService}
 * since availability only widens. It does not extend {@link StartContext}: the widening comes from
 * the marker chain, not from context inheritance.
 */
public interface RunningContext {

  /**
   * Returns a Besu-provided service that is usable once the node is running.
   *
   * <p>Every service the plugin API offers for this phase is provided, except those whose
   * documentation restricts them to certain node configurations. The marker interfaces themselves
   * are not services, so a lookup of one fails.
   *
   * @param type the service interface
   * @param <T> the service type
   * @return the service
   * @throws IllegalStateException if the service is not provided on this node; thrown from {@code
   *     afterMainLoop()}, this fails the plugin like any other error there
   */
  <T extends RunningService> T getBesuService(Class<T> type);

  /**
   * Returns a service published by another plugin through {@link
   * RegistrationContext#registerService(Class, BesuService)}.
   *
   * <p>Nothing is promised about the providing plugin's state. Plugins are called in discovery
   * order, so its {@code afterMainLoop()} may not have run yet. And when Besu runs with {@code
   * --plugin-continue-on-error}, a plugin that published a service in {@code register()} and then
   * failed in {@code start()} or {@code afterMainLoop()} is dropped, but what it published stays
   * registered: this method still returns that service, although its plugin did not complete its
   * startup and will not be stopped.
   *
   * @param type the service interface
   * @param <T> the service type
   * @return the service, or empty if no plugin published it
   * @throws IllegalArgumentException if {@code type} is a Besu-provided service, that is one marked
   *     {@link RegistrationService}, {@link StartService} or {@link RunningService}; look those up
   *     with {@code getBesuService}
   */
  <T extends BesuService> Optional<T> getPluginService(Class<T> type);
}
