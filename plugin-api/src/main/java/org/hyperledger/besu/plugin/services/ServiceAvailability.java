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
package org.hyperledger.besu.plugin.services;

import org.hyperledger.besu.plugin.ServiceLifecyclePhase;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the earliest lifecycle phase at which a service interface is fully initialized and safe
 * to use.
 *
 * <p>When a plugin requests a service via {@link
 * org.hyperledger.besu.plugin.ServiceManager#getService(Class)}, the plugin context can use this
 * annotation to detect if the service is being accessed too early and emit a warning. This helps
 * plugin developers catch lifecycle ordering bugs at development time rather than in production.
 *
 * <h3>Usage</h3>
 *
 * <pre>{@code
 * @ServiceAvailability(availableFrom = ServiceLifecyclePhase.STARTED)
 * public interface BesuEvents extends BesuService {
 *     // ...
 * }
 * }</pre>
 *
 * <p>Services annotated with {@code availableFrom = REGISTERING} are available during the {@code
 * register()} callback. Services annotated with {@code availableFrom = STARTED} are only fully
 * usable after the {@code start()} callback begins.
 *
 * <p>Note: some services (e.g. {@code BlockchainService}) are <em>registered</em> early (during
 * REGISTERING) but are not <em>fully initialized</em> until STARTED. These should be annotated with
 * {@code availableFrom = REGISTERING, fullyInitializedFrom = STARTED} to indicate the distinction.
 *
 * @see ServiceLifecyclePhase
 * @see org.hyperledger.besu.plugin.ServiceManager
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ServiceAvailability {

  /**
   * The earliest lifecycle phase at which this service is registered and can be retrieved from the
   * service manager. It may not yet be fully initialized.
   *
   * @return the phase from which the service is available
   */
  ServiceLifecyclePhase availableFrom();

  /**
   * The lifecycle phase at which this service is fully initialized and all methods are safe to
   * call. Defaults to the same value as {@link #availableFrom()}, meaning the service is fully
   * usable as soon as it is available.
   *
   * <p>When {@code fullyInitializedFrom} differs from {@code availableFrom}, the service is
   * available early for registration purposes (e.g., registering RPC endpoints or CLI options) but
   * methods that query runtime state should not be called until the fully-initialized phase.
   *
   * @return the phase from which the service is fully initialized
   */
  ServiceLifecyclePhase fullyInitializedFrom() default ServiceLifecyclePhase.UNINITIALIZED;
}
