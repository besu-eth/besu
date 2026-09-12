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

import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractTestReloadConfigurationPlugin implements BesuPlugin {
  private File callbackDir;
  private final int pluginNum;

  public AbstractTestReloadConfigurationPlugin(final int pluginNum) {
    this.pluginNum = pluginNum;
  }

  @Override
  public void defineOptions(final PicoCLIOptions options) {
    options.addPicoCLIOptions("reload-conf" + pluginNum, this);
  }

  @Override
  public void register(final ServiceManager serviceManager) {
    callbackDir = PluginCallbackDir.of(serviceManager);
  }

  protected abstract boolean shouldFail();

  @Override
  public void start() {}

  @Override
  public CompletableFuture<Void> reloadConfiguration() {
    return CompletableFuture.runAsync(this::notifyConfigurationReloaded)
        .thenAccept(
            unused -> {
              if (shouldFail()) {
                throw new RuntimeException("Failed to reload configuration");
              }
            });
  }

  @Override
  public void stop() {}

  private void notifyConfigurationReloaded() {
    try {
      final File callbackFile = new File(callbackDir, "reloadConfiguration." + pluginNum);
      if (!callbackFile.getParentFile().exists()) {
        callbackFile.getParentFile().mkdirs();
        callbackFile.getParentFile().deleteOnExit();
      }
      callbackFile.createNewFile();
      callbackFile.deleteOnExit();
    } catch (final IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
