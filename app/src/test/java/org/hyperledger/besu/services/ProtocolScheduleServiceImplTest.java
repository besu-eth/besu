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
package org.hyperledger.besu.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.ethereum.core.plugins.ImmutablePluginConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleCustomization;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleCustomizer;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleService;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class ProtocolScheduleServiceImplTest {

  private final GenesisConfigOptions config = mock(GenesisConfigOptions.class);

  @Test
  void resolvesEachProviderOnceAndMemoizesTheSingleMatch() {
    final ProtocolScheduleCustomizer inactive = mock(ProtocolScheduleCustomizer.class);
    final ProtocolScheduleCustomizer active = mock(ProtocolScheduleCustomizer.class);
    final ProtocolScheduleCustomization expected = customization("active");
    when(inactive.customize(config)).thenReturn(Optional.empty());
    when(active.customize(config)).thenReturn(Optional.of(expected));
    final ProtocolScheduleServiceImpl service = new ProtocolScheduleServiceImpl();
    service.registerProtocolScheduleCustomizer(inactive);
    service.registerProtocolScheduleCustomizer(active);

    assertThat(service.resolve(config)).isSameAs(expected);
    assertThat(service.resolve(config)).isSameAs(expected);

    verify(inactive, times(1)).customize(config);
    verify(active, times(1)).customize(config);
  }

  @Test
  void rejectsMoreThanOneCustomizerForTheActiveChainDeterministically() {
    final ProtocolScheduleServiceImpl service = new ProtocolScheduleServiceImpl();
    service.registerProtocolScheduleCustomizer(config -> Optional.of(customization("zeta")));
    service.registerProtocolScheduleCustomizer(config -> Optional.of(customization("alpha")));

    assertThatIllegalStateException()
        .isThrownBy(() -> service.resolve(config))
        .withMessageContaining("alpha, zeta");
  }

  @Test
  void rejectsRegistrationAsSoonAsResolutionBegins() {
    final ProtocolScheduleServiceImpl service = new ProtocolScheduleServiceImpl();
    service.resolve(config);

    assertThatIllegalStateException()
        .isThrownBy(
            () ->
                service.registerProtocolScheduleCustomizer(
                    ignored -> Optional.of(customization("late"))))
        .withMessageContaining("plugin registration");
  }

  @Test
  void registrationClosesWhenThePluginRegistrationPhaseEnds() {
    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    final ProtocolScheduleService service =
        context.getService(ProtocolScheduleService.class).orElseThrow();
    context.initialize(
        ImmutablePluginConfiguration.builder().externalPluginsEnabled(false).build());
    context.registerPlugins();

    assertThatIllegalStateException()
        .isThrownBy(
            () ->
                service.registerProtocolScheduleCustomizer(
                    ignored -> Optional.of(customization("late"))))
        .withMessageContaining("plugin registration");
  }

  @Test
  void besuOwnedRegistryCannotBeReplaced() {
    final BesuPluginContextImpl context = new BesuPluginContextImpl();

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                context.addService(
                    ProtocolScheduleService.class, mock(ProtocolScheduleService.class)))
        .withMessageContaining("Besu-owned");
  }

  @Test
  void ephemeryResetClearsTheFrozenRegistryBeforePluginsRegisterAgain() {
    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    final ProtocolScheduleService service =
        context.getService(ProtocolScheduleService.class).orElseThrow();
    final AtomicInteger evaluations = new AtomicInteger();
    final ProtocolScheduleCustomizer customizer =
        ignored -> {
          evaluations.incrementAndGet();
          return Optional.of(customization("ephemery"));
        };
    service.registerProtocolScheduleCustomizer(customizer);
    context.resolveProtocolScheduleCustomization(config);

    context.resetState();
    service.registerProtocolScheduleCustomizer(customizer);
    context.resolveProtocolScheduleCustomization(config);

    assertThat(evaluations).hasValue(2);
  }

  @Test
  void pluginVisibleServiceDoesNotExposeLifecycleControl() {
    // getDeclaredMethods returns them in no particular order
    assertThat(
            Arrays.stream(ProtocolScheduleService.class.getDeclaredMethods()).map(Method::getName))
        .containsExactlyInAnyOrder("registerProtocolScheduleCustomizer");
  }

  private static ProtocolScheduleCustomization customization(final String name) {
    return new ProtocolScheduleCustomization(name, List.of());
  }
}
