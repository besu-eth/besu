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

import java.util.Optional;

/**
 * The context handed to {@link BesuPlugin#start(StartContext)}.
 *
 * <p>Its lookup only accepts {@link StartService} types, so requesting a service that only works
 * once the node is running, or one that is only usable during registration, does not compile. The
 * services obtained here keep working for the rest of the node's life, so the context can be kept
 * and used from later phases and from callbacks.
 */
public interface StartContext {

  /**
   * Returns a Besu-provided service that is usable from {@code start()} onwards.
   *
   * <p>If the call compiles, the service is there. The only failure is a service that is not
   * provided on this node's configuration, which is documented on the service and fails the
   * plugin's start.
   *
   * @param type the service interface
   * @param <T> the service type
   * @return the service
   * @throws IllegalStateException if the service is not provided on this node
   */
  <T extends StartService> T getBesuService(Class<T> type);

  /**
   * Returns a service published by another plugin through {@link
   * RegistrationContext#registerService(Class, BesuService)}.
   *
   * <p>Whether the providing plugin is installed is a deployment fact Besu cannot promise, which is
   * why this is the one lookup that can answer "not here".
   *
   * @param type the service interface
   * @param <T> the service type
   * @return the service, or empty if no plugin published it
   */
  <T extends BesuService> Optional<T> getPluginService(Class<T> type);
}
