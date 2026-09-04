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
package org.hyperledger.besu.tests.acceptance.plugins;

import org.hyperledger.besu.plugin.CoreConfiguration;
import org.hyperledger.besu.plugin.ServiceManager;

import java.io.File;

/**
 * Locates the directory the acceptance tests watch for the signal files written by the test
 * plugins: {@code <data-path>/plugins}. The data path is read from {@link CoreConfiguration}, which
 * is populated before {@code register()} is called.
 */
final class PluginCallbackDir {
  private PluginCallbackDir() {}

  static File of(final ServiceManager serviceManager) {
    return serviceManager
        .getService(CoreConfiguration.class)
        .orElseThrow(() -> new IllegalStateException("CoreConfiguration service not available"))
        .getDataPath()
        .resolve("plugins")
        .toFile();
  }
}
