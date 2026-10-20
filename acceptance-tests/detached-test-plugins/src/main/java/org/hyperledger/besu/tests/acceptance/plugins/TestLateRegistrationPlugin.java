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
import org.hyperledger.besu.plugin.RegistrationClosedException;
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.StartContext;
import org.hyperledger.besu.plugin.services.HealthCheckService;
import org.hyperledger.besu.plugin.services.PermissioningService;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.RpcEndpointRegistry;
import org.hyperledger.besu.plugin.services.SecurityModuleService;
import org.hyperledger.besu.plugin.services.StorageService;
import org.hyperledger.besu.plugin.services.TransactionPoolValidatorService;
import org.hyperledger.besu.plugin.services.TransactionSelectionService;
import org.hyperledger.besu.plugin.services.TransactionValidatorService;
import org.hyperledger.besu.plugin.services.metrics.MetricCategoryRegistry;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.txselection.PluginTransactionSelectorFactory;
import org.hyperledger.besu.plugin.services.txvalidator.PluginTransactionPoolValidator;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;

/**
 * Keeps every registration-phase service from register() and registers through it again from
 * start(), once registration has closed. With {@code --Xplugin-late-registration=report} it tries
 * every write method, catches the rejection and lists the rejected methods in {@code
 * plugins/lateRegistration.rejected}; with {@code fatal} it lets one rejection escape, which must
 * stop the node. Without the option it does nothing, so the other plugin tests are unaffected.
 */
@AutoService(BesuPlugin.class)
public class TestLateRegistrationPlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(TestLateRegistrationPlugin.class);
  private static final String UNSET = "UNSET";
  private static final String REPORT = "report";
  private static final String FATAL = "fatal";
  private static final String REJECTED_FILE = "lateRegistration.rejected";

  @Option(names = "--Xplugin-late-registration", hidden = true, defaultValue = UNSET)
  String mode = UNSET;

  private File callbackDir;

  // the late registrations, keyed by the method each one calls on a service kept from register()
  private final Map<String, Runnable> lateRegistrations = new LinkedHashMap<>();

  @Override
  public void defineOptions(final PicoCLIOptions options) {
    options.addPicoCLIOptions("late", this);
  }

  @Override
  public void register(final RegistrationContext context) {
    // inert unless the option selects a mode: the lookups below, and the RocksDB factory they
    // depend on, are not something every other plugin test should run
    if (UNSET.equals(mode)) {
      return;
    }
    callbackDir = PluginCallbackDir.resolve(context);

    final StorageService storage = context.getBesuService(StorageService.class);
    // the built-in RocksDB factory registers before external plugins, so it is there to
    // re-register; the call must be rejected before the argument matters
    final KeyValueStorageFactory rocksDb = storage.getByName("rocksdb").orElseThrow();
    lateRegistrations.put(
        "registerKeyValueStorage", () -> storage.registerKeyValueStorage(rocksDb));

    final SecurityModuleService securityModules =
        context.getBesuService(SecurityModuleService.class);
    lateRegistrations.put("register", () -> securityModules.register("late", () -> null));

    final RpcEndpointRegistry rpcEndpoints = context.getBesuService(RpcEndpointRegistry.class);
    lateRegistrations.put(
        "registerRPCEndpoint",
        () -> rpcEndpoints.registerRPCEndpoint("late", "call", request -> null));

    final TransactionSelectionService selectors =
        context.getBesuService(TransactionSelectionService.class);
    lateRegistrations.put(
        "registerPluginTransactionSelectorFactory",
        () ->
            selectors.registerPluginTransactionSelectorFactory(
                new PluginTransactionSelectorFactory() {}));

    final TransactionPoolValidatorService poolValidators =
        context.getBesuService(TransactionPoolValidatorService.class);
    lateRegistrations.put(
        "registerPluginTransactionValidatorFactory",
        () ->
            poolValidators.registerPluginTransactionValidatorFactory(
                () -> PluginTransactionPoolValidator.VALIDATE_ALL));

    final TransactionValidatorService validators =
        context.getBesuService(TransactionValidatorService.class);
    lateRegistrations.put(
        "registerTransactionValidatorRule",
        () -> validators.registerTransactionValidatorRule(transaction -> Optional.empty()));

    final PermissioningService permissioning = context.getBesuService(PermissioningService.class);
    lateRegistrations.put(
        "registerNodePermissioningProvider",
        () -> permissioning.registerNodePermissioningProvider((source, destination) -> true));
    lateRegistrations.put(
        "registerTransactionPermissioningProvider",
        () -> permissioning.registerTransactionPermissioningProvider(transaction -> true));
    lateRegistrations.put(
        "registerNodeMessagePermissioningProvider",
        () -> permissioning.registerNodeMessagePermissioningProvider((destination, code) -> true));

    final HealthCheckService healthChecks = context.getBesuService(HealthCheckService.class);
    lateRegistrations.put(
        "registerHealthCheck",
        () ->
            healthChecks.registerHealthCheck(
                "/late", params -> HealthCheckService.HealthCheckResult.of(true)));
    lateRegistrations.put(
        "unregisterHealthCheck", () -> healthChecks.unregisterHealthCheck("/late"));

    final MetricCategoryRegistry metricCategories =
        context.getBesuService(MetricCategoryRegistry.class);
    lateRegistrations.put(
        "addMetricCategory",
        () ->
            metricCategories.addMetricCategory(
                TestMetricsPlugin.TestMetricCategory.TEST_METRIC_CATEGORY));
  }

  @Override
  public void start(final StartContext context) {
    if (FATAL.equals(mode)) {
      LOG.info("Registering late and letting the rejection escape");
      lateRegistrations.get("registerKeyValueStorage").run();
    } else if (REPORT.equals(mode)) {
      LOG.info("Registering late through every registration-phase service");
      final List<String> rejected = new ArrayList<>();
      lateRegistrations.forEach(
          (method, registration) -> {
            try {
              registration.run();
            } catch (final RegistrationClosedException e) {
              rejected.add(method);
            }
          });
      PluginCallbackDir.write(callbackDir, REJECTED_FILE, rejected);
    }
  }

  @Override
  public void stop() {}
}
