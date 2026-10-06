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
import org.hyperledger.besu.plugin.storage.StorageConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(BesuPlugin.class)
public class TestStorageConfigurationPlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(TestStorageConfigurationPlugin.class);
  private ServiceManager serviceManager;
  private File callbackDir;

  @Override
  public void register(final ServiceManager serviceManager) {
    LOG.info("Registering TestStorageConfigurationPlugin");
    this.serviceManager = serviceManager;
    callbackDir = new File(System.getProperty("besu.plugins.dir", "plugins"));
  }

  @Override
  public void start() {
    LOG.info("Starting TestStorageConfigurationPlugin");
    final var storageConfiguration =
        serviceManager.getService(StorageConfiguration.class).orElseThrow();
    writeStoragePath(storageConfiguration.getStoragePath().toString());
  }

  @Override
  public void stop() {}

  private void writeStoragePath(final String storagePath) {
    try {
      final File callbackFile = new File(callbackDir, "storageConfiguration.storagePath");
      if (!callbackFile.getParentFile().exists()) {
        callbackFile.getParentFile().mkdirs();
        callbackFile.getParentFile().deleteOnExit();
      }
      Files.writeString(callbackFile.toPath(), storagePath);
      callbackFile.deleteOnExit();
    } catch (final IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
