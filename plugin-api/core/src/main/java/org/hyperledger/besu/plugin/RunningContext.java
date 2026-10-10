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
   * <p>If the call compiles, the service is there. The only failure is a service that is not
   * provided on this node's configuration, which is documented on the service.
   *
   * @param type the service interface
   * @param <T> the service type
   * @return the service
   * @throws IllegalStateException if the service is not provided on this node
   */
  <T extends RunningService> T getBesuService(Class<T> type);

  /**
   * Returns a service published by another plugin through {@link
   * RegistrationContext#registerService(Class, BesuService)}.
   *
   * @param type the service interface
   * @param <T> the service type
   * @return the service, or empty if no plugin published it
   */
  <T extends BesuService> Optional<T> getPluginService(Class<T> type);
}
