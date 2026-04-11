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

/**
 * Represents the lifecycle phases of the Besu plugin system.
 *
 * <p>Plugins progress through these phases in order during Besu startup and shutdown. The phase
 * determines which services are available via {@link ServiceManager#getService(Class)}.
 *
 * <p>Services registered during early phases (e.g., {@link #REGISTERING}) include CLI options,
 * storage, and security modules. Services that depend on a running node (e.g., P2P, events,
 * metrics) only become available during {@link #BEFORE_MAIN_LOOP_STARTED} and later.
 */
public enum PluginLifecyclePhase {
  /** System has not yet been initialized. No services are available. */
  UNINITIALIZED,
  /** Plugin configuration has been loaded. No services are available yet. */
  INITIALIZED,
  /**
   * Plugins are being registered. Services available during this phase include {@code
   * PicoCLIOptions}, {@code StorageService}, {@code SecurityModuleService}, {@code
   * MetricCategoryRegistry}, {@code PermissioningService}, {@code RpcEndpointService}, {@code
   * BlockchainService}, and transaction-related services.
   */
  REGISTERING,
  /** All plugins have been registered. Same services as {@link #REGISTERING}. */
  REGISTERED,
  /** The {@code beforeExternalServices()} callback is being invoked on each plugin. */
  BEFORE_EXTERNAL_SERVICES_STARTED,
  /** All {@code beforeExternalServices()} callbacks have completed. */
  BEFORE_EXTERNAL_SERVICES_FINISHED,
  /**
   * Plugins are being started. Additional services become available during this phase, including
   * {@code BesuEvents}, {@code MetricsSystem}, {@code P2PService}, {@code WorldStateService},
   * {@code SynchronizationService}, {@code TraceService}, {@code MiningService}, and {@code
   * BlockSimulationService}.
   */
  BEFORE_MAIN_LOOP_STARTED,
  /** All plugins have been started. All services are available. */
  BEFORE_MAIN_LOOP_FINISHED,
  /** The {@code afterExternalServicePostMainLoop()} callback is being invoked on each plugin. */
  AFTER_EXTERNAL_SERVICES_POST_MAIN_LOOP,
  /** The system is shutting down. Plugins are being stopped. */
  STOPPING,
  /** All plugins have been stopped. */
  STOPPED
}
