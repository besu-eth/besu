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
package org.hyperledger.besu.plugin.services.health;

import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.StartContext;
import org.hyperledger.besu.plugin.services.HealthCheckService;

/** The liveness check plugin. */
public class LivenessCheckPlugin implements BesuPlugin {

  private static final String LIVENESS_ENDPOINT = "/liveness";

  /** Instantiates a new liveness check plugin. */
  public LivenessCheckPlugin() {}

  @Override
  public void register(final RegistrationContext context) {
    final HealthCheckService healthCheckService = context.getBesuService(HealthCheckService.class);

    healthCheckService.registerHealthCheck(
        LIVENESS_ENDPOINT, params -> HealthCheckService.HealthCheckResult.of(true));
  }

  @Override
  public void start(final StartContext context) {}

  @Override
  public void stop() {}
}
