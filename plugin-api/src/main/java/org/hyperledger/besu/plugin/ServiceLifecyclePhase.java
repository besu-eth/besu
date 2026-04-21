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
 * Defines the lifecycle phases of the Besu plugin system, from node startup to shutdown.
 *
 * <p>Plugins progress through these phases in a strict order. Each phase determines which services
 * are available and what operations plugins are permitted to perform. Accessing a service before it
 * is fully initialized can lead to undefined behavior or {@link IllegalStateException}.
 *
 * <h3>Phase Order</h3>
 *
 * <pre>{@code
 * UNINITIALIZED → REGISTERING → REGISTERED → BEFORE_EXTERNAL_SERVICES
 *   → STARTED → STOPPING → STOPPED
 * }</pre>
 *
 * <h3>Service Availability</h3>
 *
 * <ul>
 *   <li><b>REGISTERING</b> — Configuration services available: {@code PicoCLIOptions}, {@code
 *       BesuConfiguration}, {@code SecurityModuleService}, {@code StorageService}, {@code
 *       MetricCategoryRegistry}, {@code PermissioningService}, {@code RpcEndpointService}
 *       (registration only), {@code TransactionSelectionService}, {@code
 *       TransactionPoolValidatorService}, {@code TransactionSimulationService} (registration only),
 *       {@code BlockchainService} (registration only), {@code TransactionValidatorService}.
 *   <li><b>STARTED</b> — All runtime services additionally available: {@code BesuEvents}, {@code
 *       MetricsSystem}, {@code WorldStateService}, {@code SynchronizationService}, {@code
 *       P2PService}, {@code TransactionPoolService}, {@code RlpConverterService}, {@code
 *       TraceService}, {@code MiningService}, {@code BlockSimulationService}. Services registered
 *       early (e.g. {@code BlockchainService}) are now fully initialized and safe to call.
 * </ul>
 */
public enum ServiceLifecyclePhase {
  /** The plugin system has not been initialized. No services are available. */
  UNINITIALIZED,

  /**
   * Plugins are being registered. Configuration and registration services are available. Plugins
   * should use this phase to register CLI options, storage factories, and other configuration
   * hooks.
   */
  REGISTERING,

  /** Plugin registration is complete. Same services as REGISTERING remain available. */
  REGISTERED,

  /**
   * The {@code beforeExternalServices()} callback is being executed. Services from the REGISTERING
   * phase remain available.
   */
  BEFORE_EXTERNAL_SERVICES,

  /**
   * Plugins have been started via {@code start()}. All services are available and fully
   * initialized, including runtime services like {@code BesuEvents}, {@code MetricsSystem}, {@code
   * WorldStateService}, and others.
   */
  STARTED,

  /** Plugins are being stopped. Services may become unavailable during this phase. */
  STOPPING,

  /** All plugins have been stopped. No services should be accessed. */
  STOPPED
}
