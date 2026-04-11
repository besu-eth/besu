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
package org.hyperledger.besu.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.core.plugins.ImmutablePluginConfiguration;
import org.hyperledger.besu.ethereum.core.plugins.PluginConfiguration;
import org.hyperledger.besu.plugin.PluginLifecyclePhase;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.BesuService;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BesuPluginContextImplTest {

  private BesuPluginContextImpl context;
  private PluginConfiguration config;

  interface TestServiceA extends BesuService {}

  interface TestServiceB extends BesuService {}

  @BeforeEach
  void setUp() {
    context = new BesuPluginContextImpl();
    config = ImmutablePluginConfiguration.builder().externalPluginsEnabled(false).build();
  }

  @Test
  void initialLifecyclePhaseIsUninitialized() {
    assertThat(context.getLifecyclePhase()).isEqualTo(PluginLifecyclePhase.UNINITIALIZED);
  }

  @Test
  void lifecyclePhaseTransitionsToInitializedAfterInitialize() {
    context.initialize(config);
    assertThat(context.getLifecyclePhase()).isEqualTo(PluginLifecyclePhase.INITIALIZED);
  }

  @Test
  void lifecyclePhaseTransitionsToRegisteredAfterRegisterPlugins() {
    context.initialize(config);
    context.registerPlugins();
    assertThat(context.getLifecyclePhase()).isEqualTo(PluginLifecyclePhase.REGISTERED);
  }

  @Test
  void lifecyclePhaseTransitionsToBeforeExternalServicesFinished() {
    context.initialize(config);
    context.registerPlugins();
    context.beforeExternalServices();
    assertThat(context.getLifecyclePhase())
        .isEqualTo(PluginLifecyclePhase.BEFORE_EXTERNAL_SERVICES_FINISHED);
  }

  @Test
  void lifecyclePhaseTransitionsToBeforeMainLoopFinished() {
    context.initialize(config);
    context.registerPlugins();
    context.beforeExternalServices();
    context.startPlugins();
    assertThat(context.getLifecyclePhase())
        .isEqualTo(PluginLifecyclePhase.BEFORE_MAIN_LOOP_FINISHED);
  }

  @Test
  void lifecyclePhaseTransitionsToAfterExternalServicesPostMainLoop() {
    context.initialize(config);
    context.registerPlugins();
    context.beforeExternalServices();
    context.startPlugins();
    context.afterExternalServicesMainLoop();
    assertThat(context.getLifecyclePhase())
        .isEqualTo(PluginLifecyclePhase.AFTER_EXTERNAL_SERVICES_POST_MAIN_LOOP);
  }

  @Test
  void afterExternalServicesMainLoopCannotBeCalledTwice() {
    context.initialize(config);
    context.registerPlugins();
    context.beforeExternalServices();
    context.startPlugins();
    context.afterExternalServicesMainLoop();

    assertThatThrownBy(() -> context.afterExternalServicesMainLoop())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void stopPluginsWorksAfterAfterExternalServicesMainLoop() {
    context.initialize(config);
    context.registerPlugins();
    context.beforeExternalServices();
    context.startPlugins();
    context.afterExternalServicesMainLoop();
    context.stopPlugins();
    assertThat(context.getLifecyclePhase()).isEqualTo(PluginLifecyclePhase.STOPPED);
  }

  @Test
  void stopPluginsWorksDirectlyFromBeforeMainLoopFinished() {
    context.initialize(config);
    context.registerPlugins();
    context.beforeExternalServices();
    context.startPlugins();
    context.stopPlugins();
    assertThat(context.getLifecyclePhase()).isEqualTo(PluginLifecyclePhase.STOPPED);
  }

  @Test
  void lifecyclePhaseIsExposedThroughServiceManagerInterface() {
    ServiceManager serviceManager = context;
    assertThat(serviceManager.getLifecyclePhase()).isEqualTo(PluginLifecyclePhase.UNINITIALIZED);

    context.initialize(config);
    assertThat(serviceManager.getLifecyclePhase()).isEqualTo(PluginLifecyclePhase.INITIALIZED);
  }

  @Test
  void resetStateSetsPhaseToUninitialized() {
    context.initialize(config);
    context.registerPlugins();
    assertThat(context.getLifecyclePhase()).isEqualTo(PluginLifecyclePhase.REGISTERED);

    context.resetState();
    assertThat(context.getLifecyclePhase()).isEqualTo(PluginLifecyclePhase.UNINITIALIZED);
  }

  @Test
  void serviceRegistrySupportsBasicAddAndGet() {
    final TestServiceA serviceA = new TestServiceA() {};
    context.addService(TestServiceA.class, serviceA);

    final Optional<TestServiceA> retrieved = context.getService(TestServiceA.class);
    assertThat(retrieved).isPresent().contains(serviceA);
  }

  @Test
  void getServiceReturnsEmptyForUnregisteredService() {
    final Optional<TestServiceA> retrieved = context.getService(TestServiceA.class);
    assertThat(retrieved).isEmpty();
  }

  @Test
  void multipleServicesCanBeRegisteredAndRetrieved() {
    final TestServiceA serviceA = new TestServiceA() {};
    final TestServiceB serviceB = new TestServiceB() {};

    context.addService(TestServiceA.class, serviceA);
    context.addService(TestServiceB.class, serviceB);

    assertThat(context.getService(TestServiceA.class)).isPresent().contains(serviceA);
    assertThat(context.getService(TestServiceB.class)).isPresent().contains(serviceB);
  }

  @Test
  void defaultServiceManagerReturnsUninitializedPhase() {
    ServiceManager defaultManager = new ServiceManager.SimpleServiceManager();
    assertThat(defaultManager.getLifecyclePhase()).isEqualTo(PluginLifecyclePhase.UNINITIALIZED);
  }
}
