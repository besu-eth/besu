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
import org.hyperledger.besu.plugin.services.RpcEndpointRegistry;

import com.google.auto.service.AutoService;

/**
 * Registers an RPC method and records whether it was invoked before this plugin's start() returned.
 * It loads after {@link TestEarlyInProcessCallPlugin}, which calls the method from its own start().
 */
@AutoService(BesuPlugin.class)
public class TestEarlyInProcessTargetPlugin implements BesuPlugin {
  private volatile boolean started;
  private volatile boolean invokedBeforeStart;

  @Override
  public void register(final RegistrationContext context) {
    final RpcEndpointRegistry registry = context.getBesuService(RpcEndpointRegistry.class);
    registry.registerRPCEndpoint(
        "tests",
        "earlyTarget",
        request -> {
          if (!started) {
            invokedBeforeStart = true;
          }
          return "ok";
        });
    registry.registerRPCEndpoint(
        "tests", "earlyTargetInvokedBeforeStart", request -> invokedBeforeStart);
  }

  @Override
  public void start(final StartContext context) {
    started = true;
  }

  @Override
  public void stop() {}
}
