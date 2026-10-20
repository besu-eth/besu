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
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.StartContext;
import org.hyperledger.besu.plugin.services.HealthCheckService;

import java.io.File;
import java.util.List;
import java.util.Map;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(BesuPlugin.class)
public class TestHealthCheckPlugin implements BesuPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(TestHealthCheckPlugin.class);
  private static final String LIVENESS_ENDPOINT = "/liveness";
  private static final String READINESS_ENDPOINT = "/readiness";
  private static final String LIVENESS_CALLED_FILE = "liveness-plugin-called";
  private static final String READINESS_CALLED_FILE = "readiness-plugin-called";

  private File callbackDir;

  @Override
  public void register(final RegistrationContext context) {
    LOG.info("Registered TestHealthCheckPlugin");
    callbackDir = PluginCallbackDir.resolve(context);
    final HealthCheckService healthCheckService = context.getBesuService(HealthCheckService.class);
    healthCheckService.registerHealthCheck(
        LIVENESS_ENDPOINT,
        params -> {
          markCalled(LIVENESS_CALLED_FILE);
          return new HealthCheckService.HealthCheckResult(true, Map.of("testPlugin", "liveness"));
        });
    healthCheckService.registerHealthCheck(
        READINESS_ENDPOINT,
        params -> {
          markCalled(READINESS_CALLED_FILE);
          return new HealthCheckService.HealthCheckResult(true, Map.of("testPlugin", "readiness"));
        });
  }

  @Override
  public void start(final StartContext context) {}

  @Override
  public void stop() {}

  private void markCalled(final String fileName) {
    PluginCallbackDir.write(callbackDir, fileName, List.of("plugin-called"));
    LOG.info("Health endpoint called by plugin, marker written to {}/{}", callbackDir, fileName);
  }
}
