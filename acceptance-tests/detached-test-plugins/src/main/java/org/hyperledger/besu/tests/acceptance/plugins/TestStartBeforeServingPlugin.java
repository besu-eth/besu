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
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.RpcEndpointRegistry;
import org.hyperledger.besu.plugin.services.rpc.PluginRpcRequest;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;

/**
 * Answers {@code tests_isStarted} with whether its {@code start()} has returned. With {@code
 * --plugin-start-delay-test-ms} the start takes that long, which widens the window in which a
 * request could reach the handler before the plugin has started. The delay is off by default, so
 * the other plugin tests, which load this plugin too, do not pay for it.
 */
@AutoService(BesuPlugin.class)
public class TestStartBeforeServingPlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(TestStartBeforeServingPlugin.class);

  @Option(names = "--plugin-start-delay-test-ms")
  long startDelayMs = 0;

  private volatile boolean started;

  @Override
  public void defineOptions(final PicoCLIOptions options) {
    options.addPicoCLIOptions("start-delay", this);
  }

  @Override
  public void register(final RegistrationContext context) {
    context
        .getBesuService(RpcEndpointRegistry.class)
        .registerRPCEndpoint("tests", "isStarted", this::isStarted);
  }

  private Boolean isStarted(final PluginRpcRequest request) {
    return started;
  }

  @Override
  public void start(final StartContext context) {
    if (startDelayMs > 0) {
      LOG.info("Delaying start by {} ms", startDelayMs);
      try {
        Thread.sleep(startDelayMs);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while delaying start", e);
      }
    }
    started = true;
  }

  @Override
  public void stop() {}
}
