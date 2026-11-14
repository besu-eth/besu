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

import java.io.File;
import java.util.List;

/**
 * Records when it starts and stops: {@code stopOrder.<letter>.started} once its {@code start()}
 * ran, and its letter appended to {@code stopOrder} from {@code stop()}, so the acceptance tests
 * can read the order in which Besu stopped the plugins of a node. The plugins of one jar start in
 * class name order, so A starts before B.
 */
abstract class AbstractTestStopOrderPlugin implements BesuPlugin {
  static final String STOP_ORDER_FILE = "stopOrder";

  private File callbackDir;

  /** The letter this plugin writes for itself. */
  abstract String letter();

  @Override
  public void register(final RegistrationContext context) {
    callbackDir = PluginCallbackDir.resolve(context);
  }

  @Override
  public void start(final StartContext context) {
    // kept past the JVM exit: a test reads it after a failed start has stopped the node
    PluginCallbackDir.writeKept(
        callbackDir, STOP_ORDER_FILE + "." + letter() + ".started", List.of("started"));
  }

  @Override
  public void stop() {
    PluginCallbackDir.append(callbackDir, STOP_ORDER_FILE, letter());
  }
}
