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
import org.hyperledger.besu.plugin.services.InProcessRpcService;
import org.hyperledger.besu.plugin.services.RpcEndpointRegistry;
import org.hyperledger.besu.plugin.services.rpc.PluginRpcResponse;

import java.io.File;
import java.util.List;

import com.google.auto.service.AutoService;

/**
 * Registers an RPC method and records whether it was invoked before this plugin's start() returned.
 * It loads after {@link TestEarlyInProcessCallPlugin}, which calls the method from its own start().
 * Loading after it, this plugin stops before it: from stop() it calls that plugin's method
 * in-process, while that plugin is still running, and writes the outcome to a signal file that
 * outlives the node.
 */
@AutoService(BesuPlugin.class)
public class TestEarlyInProcessTargetPlugin implements BesuPlugin {
  private volatile boolean started;
  private volatile boolean invokedBeforeStart;
  private InProcessRpcService inProcessRpcService;
  private File callbackDir;

  @Override
  public void register(final RegistrationContext context) {
    callbackDir = PluginCallbackDir.resolve(context);
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
    inProcessRpcService = context.getBesuService(InProcessRpcService.class);
    started = true;
  }

  @Override
  public void stop() {
    if (!inProcessRpcService.isEnabled()) {
      return;
    }
    String outcome;
    try {
      final PluginRpcResponse response =
          inProcessRpcService.call("tests_earlyCallOutcome", new Object[0]);
      outcome = response.getType() + ": " + response.getResult();
    } catch (final RuntimeException e) {
      outcome = "EXCEPTION: " + e;
    }
    PluginCallbackDir.writeKept(callbackDir, "earlyInProcessTarget.stop", List.of(outcome));
  }
}
