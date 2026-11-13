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
import org.hyperledger.besu.plugin.StartContext;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;

import com.google.auto.service.AutoService;
import picocli.CommandLine.Option;

/**
 * The second of the two stop-order plugins: it starts after A and records "B". With {@code
 * --Xplugin-stop-order-b-test=failStart} its {@code start()} throws, so the plugins started before
 * it must be stopped by the failed startup; with {@code errorOnStop} its {@code stop()} throws an
 * {@link Error}, so the plugins stopped after it must still be stopped. Off by default.
 */
@AutoService(BesuPlugin.class)
public class TestStopOrderPluginB extends AbstractTestStopOrderPlugin {
  private static final String FAIL_START = "failStart";
  private static final String ERROR_ON_STOP = "errorOnStop";

  @Option(names = "--Xplugin-stop-order-b-test", hidden = true)
  String test = "";

  @Override
  String letter() {
    return "B";
  }

  @Override
  public void defineOptions(final PicoCLIOptions options) {
    options.addPicoCLIOptions("stop-order-b", this);
  }

  @Override
  public void start(final StartContext context) {
    if (FAIL_START.equals(test)) {
      throw new IllegalStateException("I was told to fail at start");
    }
    super.start(context);
  }

  @Override
  public void stop() {
    if (ERROR_ON_STOP.equals(test)) {
      throw new NoClassDefFoundError("test/Missing");
    }
    super.stop();
  }
}
