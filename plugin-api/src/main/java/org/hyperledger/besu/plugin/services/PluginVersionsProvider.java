/*
 * Copyright ConsenSys AG.
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

import java.util.Map;

/**
 * The interface Plugin versions provider.
 *
 * @deprecated This type is scheduled for removal from the plugin API in a future release, with no
 *     replacement. It is used only internally to print plugin versions for {@code --version}.
 */
@Deprecated(forRemoval = true)
public interface PluginVersionsProvider {
  /**
   * Gets plugin versions by name.
   *
   * @return the plugin versions
   */
  Map<String, String> getPluginVersions();
}
