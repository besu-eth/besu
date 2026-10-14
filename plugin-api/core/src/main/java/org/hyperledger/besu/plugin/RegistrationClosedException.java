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
 * Thrown when a plugin registers something after registration has closed: through a {@link
 * RegistrationService} kept in a field, or through {@link
 * RegistrationContext#registerService(Class, org.hyperledger.besu.plugin.services.BesuService)}.
 *
 * <p>Registrations belong to {@link BesuPlugin#register(RegistrationContext)}: Besu wires them into
 * the node after every plugin has returned from {@code register()}. A registration made later
 * would, depending on the service, never be read or take effect at an arbitrary point, so the
 * service rejects it with this exception. Reading from a kept {@link RegistrationService} is not
 * affected. A plugin that needs to register something must do so from {@code register()}.
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
