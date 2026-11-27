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
import org.hyperledger.besu.plugin.RunningContext;
import org.hyperledger.besu.plugin.StartContext;
import org.hyperledger.besu.plugin.services.InProcessRpcService;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.RpcEndpointRegistry;
import org.hyperledger.besu.plugin.services.rpc.PluginRpcResponse;

import java.io.File;
import java.util.List;

import com.google.auto.service.AutoService;
import picocli.CommandLine.Option;

/**
 * Calls {@link TestEarlyInProcessTargetPlugin}'s method in-process from start(), before that plugin
 * has started, and again from afterMainLoop(), and reports both outcomes through an RPC method. It
 * calls once more from stop(), when the target, which loads after it and so stops before it, has
 * already stopped, and writes that outcome to a signal file that outlives the node.
 */
@AutoService(BesuPlugin.class)
public class TestEarlyInProcessCallPlugin implements BesuPlugin {
  @Option(names = "--plugin-early-in-process-call-enabled")
  boolean enabled = false;

  private InProcessRpcService inProcessRpcService;
  private File callbackDir;
  private volatile String outcomeFromStart = "not called";
  private volatile String outcomeFromAfterMainLoop = "not called";

  @Override
  public void defineOptions(final PicoCLIOptions options) {
    options.addPicoCLIOptions("early-in-process-call", this);
  }

  @Override
  public void register(final RegistrationContext context) {
    callbackDir = PluginCallbackDir.resolve(context);
    context
        .getBesuService(RpcEndpointRegistry.class)
        .registerRPCEndpoint(
            "tests",
            "earlyCallOutcome",
            request -> List.of(outcomeFromStart, outcomeFromAfterMainLoop));
  }

  @Override
  public void start(final StartContext context) {
    inProcessRpcService = context.getBesuService(InProcessRpcService.class);
    if (enabled) {
      outcomeFromStart = callTarget();
    }
  }

  @Override
  public void afterMainLoop(final RunningContext context) {
    if (enabled) {
      outcomeFromAfterMainLoop = callTarget();
    }
  }

  @Override
  public void stop() {
    if (enabled) {
      PluginCallbackDir.writeKept(callbackDir, "earlyInProcessCall.stop", List.of(callTarget()));
    }
  }

  private String callTarget() {
    try {
      final PluginRpcResponse response =
          inProcessRpcService.call("tests_earlyTarget", new Object[0]);
      return response.getType() + ": " + response.getResult();
    } catch (final RuntimeException e) {
      return "EXCEPTION: " + e;
    }
  }
}
