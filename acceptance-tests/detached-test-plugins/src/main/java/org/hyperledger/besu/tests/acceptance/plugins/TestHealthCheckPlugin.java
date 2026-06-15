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
import org.hyperledger.besu.plugin.services.HealthCheckService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(BesuPlugin.class)
public class TestHealthCheckPlugin implements BesuPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(TestHealthCheckPlugin.class);
  private static final String CUSTOM_ENDPOINT = "/custom-health";
  private static final String LIVENESS_ENDPOINT = "/liveness";
  private static final String LIVENESS_CALLED_FILE = "liveness-plugin-called";

  private Path dataDir;

  @Override
  public void register(final ServiceManager context) {
    LOG.info("Registered TestHealthCheckPlugin");
    dataDir = Path.of(System.getProperty("besu.plugins.dir", "plugins"));
    context
        .getService(HealthCheckService.class)
        .ifPresent(
            healthCheckService -> {
              healthCheckService.registerHealthCheck(CUSTOM_ENDPOINT, params -> true);
              healthCheckService.registerHealthCheck(
                  LIVENESS_ENDPOINT,
                  params -> {
                    markLivenessCalled();
                    return true;
                  });
            });
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  private void markLivenessCalled() {
    try {
      final File markerFile = new File(dataDir.toFile(), LIVENESS_CALLED_FILE);
      Files.write(markerFile.toPath(), Collections.singletonList("plugin-called"));
      markerFile.deleteOnExit();
      LOG.info("Liveness endpoint called by plugin, marker written to {}", markerFile.toPath());
    } catch (final IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
