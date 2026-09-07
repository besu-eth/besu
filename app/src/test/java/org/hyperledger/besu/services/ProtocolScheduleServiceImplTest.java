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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.ethereum.core.plugins.ImmutablePluginConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleCustomization;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleCustomizer;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleService;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;

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
        .withMessageContaining("after registration closed");
  }

  @Test
  void registrationClosesWhenThePluginRegistrationPhaseEnds() {
    final BesuPluginContextImpl context = registeringContext();
    final ProtocolScheduleService service = serviceFrom(context);
    context.registerPlugins();
    context.endRegistration();

    assertThatIllegalStateException()
        .isThrownBy(
            () ->
                service.registerProtocolScheduleCustomizer(
                    ignored -> Optional.of(customization("late"))))
        .withMessageContaining("after registration closed");
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
  void aPluginCannotPublishItsOwnRegistry() {
    final BesuPluginContextImpl context = registeringContext();
    final RegistrationContext registration = context.registrationContextFor(mock(BesuPlugin.class));

    assertThatThrownBy(
            () ->
                registration.registerService(
                    ProtocolScheduleService.class, mock(ProtocolScheduleService.class)))
        .isInstanceOf(UnrecoverablePluginException.class)
        .hasMessageContaining("Besu-provided");
  }

  @Test
  void ephemeryResetClearsTheFrozenRegistryBeforePluginsRegisterAgain() {
    final BesuPluginContextImpl context = registeringContext();
    final ProtocolScheduleService service = serviceFrom(context);
    final AtomicInteger evaluations = new AtomicInteger();
    final ProtocolScheduleCustomizer customizer =
        ignored -> {
          evaluations.incrementAndGet();
          return Optional.of(customization("ephemery"));
        };
    service.registerProtocolScheduleCustomizer(customizer);
    context.endRegistration();
    context.resolveProtocolScheduleCustomization(config);

    context.resetState();
    context.beginRegistration();
    service.registerProtocolScheduleCustomizer(customizer);
    context.endRegistration();
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

  /** A context with external plugins disabled, opened for registration. */
  private static BesuPluginContextImpl registeringContext() {
    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.initialize(
        ImmutablePluginConfiguration.builder().externalPluginsEnabled(false).build());
    context.defineOptions(mock(PicoCLIOptions.class));
    context.beginRegistration();
    return context;
  }

  /** The registry as a plugin sees it, from its registration context. */
  private static ProtocolScheduleService serviceFrom(final BesuPluginContextImpl context) {
    return context
        .registrationContextFor(mock(BesuPlugin.class))
        .getBesuService(ProtocolScheduleService.class);
  }

  private static ProtocolScheduleCustomization customization(final String name) {
    return new ProtocolScheduleCustomization(name, List.of());
  }
}
