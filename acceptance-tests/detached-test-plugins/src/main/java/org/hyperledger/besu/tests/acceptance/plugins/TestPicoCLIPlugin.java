/*
 * Copyright contributors to Hyperledger Besu.
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
import org.hyperledger.besu.plugin.services.PicoCLIOptions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;

@AutoService(BesuPlugin.class)
public class TestPicoCLIPlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(TestPicoCLIPlugin.class);

  private static final String UNSET = "UNSET";
  private static final String FAIL_REGISTER = "FAILREGISTER";
  private static final String FAIL_BEFORE_EXTERNAL_SERVICES = "FAILBEFOREEXTERNALSERVICES";
  private static final String FAIL_START = "FAILSTART";
  private static final String FAIL_AFTER_EXTERNAL_SERVICE_POST_MAIN_LOOP =
      "FAILAFTEREXTERNALSERVICEPOSTMAINLOOP";
  private static final String FAIL_STOP = "FAILSTOP";
  private static final String PLUGIN_LIFECYCLE_PREFIX = "pluginLifecycle.";

  @Option(
      names = {"--Xplugin-test-option"},
      hidden = true,
      defaultValue = UNSET)
  String testOption = System.getProperty("testPicoCLIPlugin.testOption");

  @Option(
      names = {"--plugin-test-stable-option"},
      hidden = true,
      defaultValue = UNSET)
  String stableOption = "";

  private String state = "uninited";
  private File callbackDir;

  @Override
  public void register(final ServiceManager context) {
    LOG.info("Registering.  Test Option is '{}'", testOption);
    state = "registering";

    if (FAIL_REGISTER.equals(testOption)) {
      state = "failregister";
      throw new RuntimeException("I was told to fail at registration");
    }

    context
        .getService(PicoCLIOptions.class)
        .ifPresent(picoCLIOptions -> picoCLIOptions.addPicoCLIOptions("test", this));

    callbackDir = new File(System.getProperty("besu.plugins.dir", "plugins"));
    writeSignal("registered");
    state = "registered";
  }

  @Override
  public void beforeExternalServices() {
    LOG.info("Before external services. Test Option is '{}'", testOption);
    state = "beforeExternalServices";

    if (FAIL_BEFORE_EXTERNAL_SERVICES.equals(testOption)) {
      state = "failbeforeExternalServices";
      throw new RuntimeException("I was told to fail before external services");
    }

    writeSignal("beforeExternalServices");
    state = "beforeExternalServicesFinished";
  }

  @Override
  public void start() {
    LOG.info("Starting.  Test Option is '{}'", testOption);
    state = "starting";

    if (FAIL_START.equals(testOption)) {
      state = "failstart";
      throw new RuntimeException("I was told to fail at startup");
    }

    writeSignal("started");
    state = "started";
  }

  @Override
  public void afterMainLoop() {
    LOG.info("After main loop. Test Option is '{}'", testOption);
    state = "afterMainLoop";

    if (FAIL_AFTER_MAIN_LOOP.equals(testOption)) {
      state = "failafterMainLoop";
      throw new RuntimeException("I was told to fail after main loop");
    }

    writeSignal("afterMainLoop");
    state = "afterMainLoopFinished";
  }

  @Override
  public void stop() {
    LOG.info("Stopping.  Test Option is '{}'", testOption);
    state = "stopping";

    if (FAIL_STOP.equals(testOption)) {
      state = "failstop";
      throw new RuntimeException("I was told to fail at stop");
    }

    writeSignal("stopped");
    state = "stopped";
  }

  /** State is used to signal unit tests about the lifecycle */
  public String getState() {
    return state;
  }

  /** This is used to signal to the acceptance test that certain tasks were completed. */
  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void writeSignal(final String signal) {
    try {
      final File callbackFile = new File(callbackDir, PLUGIN_LIFECYCLE_PREFIX + signal);
      if (!callbackFile.getParentFile().exists()) {
        callbackFile.getParentFile().mkdirs();
        callbackFile.getParentFile().deleteOnExit();
      }
      Files.write(
          callbackFile.toPath(),
          Collections.singletonList(
              signal + "\ntestOption=" + testOption + "\nid=" + System.identityHashCode(this)));
      callbackFile.deleteOnExit();
    } catch (final IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
