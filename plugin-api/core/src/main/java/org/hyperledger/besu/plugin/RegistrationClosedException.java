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

/**
 * Thrown when a plugin uses a {@link RegistrationService} outside {@link
 * BesuPlugin#register(RegistrationContext)}.
 *
 * <p>Besu reads every registration once, after the last plugin has returned from {@code
 * register()}, so a registration made later would be stored and never read. Rather than silently
 * accepting it, the service rejects it with this exception. A plugin that needs to register
 * something must do so from {@code register()}.
 */
public class RegistrationClosedException extends IllegalStateException {

  /**
   * Creates the exception.
   *
   * @param serviceName the simple name of the service that was used
   * @param methodName the method that was called
   */
  public RegistrationClosedException(final String serviceName, final String methodName) {
    super(
        serviceName
            + "."
            + methodName
            + " was called after registration closed; registrations must be made from"
            + " BesuPlugin.register()");
  }
}
