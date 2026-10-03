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
package org.hyperledger.besu.ethereum.core.plugins;

import org.hyperledger.besu.plugin.services.BesuService;

import java.util.Map;
import java.util.Optional;

/**
 * Read-only view of the services that plugins published during registration, for Besu's own code to
 * consume.
 *
 * <p>Besu constructs its own services and never needs to read them back; the only registry entries
 * it consumes are the ones plugins publish, such as a {@code TrieLogService} or a {@code
 * BlockImportTracerProvider}. Whether a plugin published a given type is a deployment fact, which
 * is why the lookup returns {@link Optional}. The view is taken after registration has closed, so
 * what it returns is complete and does not change.
 */
public interface PluginProvidedServices {

  /** A view with no plugin-provided services. */
  PluginProvidedServices NONE =
      new PluginProvidedServices() {
        @Override
        public <T extends BesuService> Optional<T> lookup(final Class<T> type) {
          return Optional.empty();
        }
      };

  /**
   * Returns the service a plugin published under the given type, if any.
   *
   * @param type the service interface
   * @param <T> the service type
   * @return the published service, or empty if no plugin published one
   */
  <T extends BesuService> Optional<T> lookup(Class<T> type);

  /**
   * A view holding the given services, for tools and tests that supply a plugin-provided service
   * without a plugin.
   *
   * @param services the services, keyed by the interface they are published under
   * @return the view
   */
  static PluginProvidedServices of(final Map<Class<? extends BesuService>, BesuService> services) {
    final Map<Class<? extends BesuService>, BesuService> copy = Map.copyOf(services);
    return new PluginProvidedServices() {
      @Override
      public <T extends BesuService> Optional<T> lookup(final Class<T> type) {
        return Optional.ofNullable(copy.get(type)).map(type::cast);
      }
    };
  }
}
