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
package org.hyperledger.besu.services;

/**
 * A plugin lifecycle error that fails Besu startup regardless of {@code
 * --plugin-continue-on-error}, because the plugin misused the lifecycle rather than failed at
 * runtime. Recognised anywhere in the cause chain, so a plugin that wraps it cannot downgrade it.
 */
class UnrecoverablePluginException extends IllegalStateException {

  /**
   * Creates the exception.
   *
   * @param message what the plugin did wrong and how to fix it
   */
  UnrecoverablePluginException(final String message) {
    super(message);
  }
}
