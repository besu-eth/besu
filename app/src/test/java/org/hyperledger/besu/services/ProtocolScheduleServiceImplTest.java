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
import org.hyperledger.besu.plugin.RegistrationClosedException;
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.StartContext;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

class ProtocolScheduleServiceImplTest {

  private final GenesisConfigOptions config = mock(GenesisConfigOptions.class);
  private final AtomicBoolean registrationOpen = new AtomicBoolean(true);

  @Test
  void resolvesEachProviderOnceAndMemoizesTheSingleMatch() {
    final ProtocolScheduleCustomizer inactive = mock(ProtocolScheduleCustomizer.class);
    final ProtocolScheduleCustomizer active = mock(ProtocolScheduleCustomizer.class);
    final ProtocolScheduleCustomization expected = customization("active");
    when(inactive.customize(config)).thenReturn(Optional.empty());
    when(active.customize(config)).thenReturn(Optional.of(expected));
    final ProtocolScheduleServiceImpl service =
        new ProtocolScheduleServiceImpl(registrationOpen::get);
    service.registerProtocolScheduleCustomizer(inactive);
    service.registerProtocolScheduleCustomizer(active);
    registrationOpen.set(false);

    assertThat(service.resolve(config)).isSameAs(expected);
    assertThat(service.resolve(config)).isSameAs(expected);

    verify(inactive, times(1)).customize(config);
    verify(active, times(1)).customize(config);
  }

  @Test
  void rejectsMoreThanOneCustomizerForTheActiveChainDeterministically() {
    final ProtocolScheduleServiceImpl service =
        new ProtocolScheduleServiceImpl(registrationOpen::get);
    service.registerProtocolScheduleCustomizer(config -> Optional.of(customization("zeta")));
    service.registerProtocolScheduleCustomizer(config -> Optional.of(customization("alpha")));
    registrationOpen.set(false);

    assertThatIllegalStateException()
        .isThrownBy(() -> service.resolve(config))
        .withMessageContaining("alpha, zeta");
  }

  @Test
  void rejectsRegistrationOnceResolutionHasBegunEvenIfTheWindowReopens() {
    final ProtocolScheduleServiceImpl service =
        new ProtocolScheduleServiceImpl(registrationOpen::get);
    registrationOpen.set(false);
    service.resolve(config);
    // only reset() starts a new cycle; reopening the window alone must not
    registrationOpen.set(true);

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
  void refusesToResolveWhileRegistrationIsOpen() {
    final ProtocolScheduleServiceImpl service =
        new ProtocolScheduleServiceImpl(registrationOpen::get);

    assertThatIllegalStateException()
        .isThrownBy(() -> service.resolve(config))
        .withMessageContaining("registration is open");
  }

  @Test
  void aThrowingCustomizerFailsResolutionNamingIt() {
    final ProtocolScheduleServiceImpl service =
        new ProtocolScheduleServiceImpl(registrationOpen::get);
    final ProtocolScheduleCustomizer throwing = new ThrowingCustomizer();
    service.registerProtocolScheduleCustomizer(throwing);
    registrationOpen.set(false);

    assertThatIllegalStateException()
        .isThrownBy(() -> service.resolve(config))
        .withMessageContaining(ThrowingCustomizer.class.getName())
        .withMessageContaining("optional key missing")
        .withCauseInstanceOf(NullPointerException.class);
  }

  @Test
  void aCustomizerReturningNullFailsResolutionNamingIt() {
    final ProtocolScheduleServiceImpl service =
        new ProtocolScheduleServiceImpl(registrationOpen::get);
    // a mock would answer Optional.empty(); a plugin's own code can return null
    final ProtocolScheduleCustomizer returnsNull = ignored -> null;
    service.registerProtocolScheduleCustomizer(returnsNull);
    registrationOpen.set(false);

    assertThatIllegalStateException()
        .isThrownBy(() -> service.resolve(config))
        .withMessageContaining(returnsNull.getClass().getName())
        .withMessageContaining("Optional.empty()");
  }

  @Test
  void laterResolutionsReturnTheFirstWhateverConfigTheyPass() {
    final ProtocolScheduleServiceImpl service =
        new ProtocolScheduleServiceImpl(registrationOpen::get);
    final ProtocolScheduleCustomization expected = customization("first");
    service.registerProtocolScheduleCustomizer(
        candidate -> candidate == config ? Optional.of(expected) : Optional.empty());
    registrationOpen.set(false);

    assertThat(service.resolve(config)).isSameAs(expected);
    assertThat(service.resolve(mock(GenesisConfigOptions.class))).isSameAs(expected);
  }

  @Test
  void anEphemeryResetDoesNotReopenRegistrationBeforeTheWindowDoes() {
    final BesuPluginContextImpl context = registeringContext();
    final ProtocolScheduleService service = serviceFrom(context);
    context.endRegistration();
    context.resolveProtocolScheduleCustomization(config);

    context.resetState();

    assertThatThrownBy(
            () ->
                service.registerProtocolScheduleCustomizer(
                    ignored -> Optional.of(customization("between cycles"))))
        .isInstanceOf(RegistrationClosedException.class);
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

  @Test
  void customizersOfAPluginThatFailsToRegisterAreDiscarded() {
    final ProtocolScheduleCustomizer healthyCustomizer = mock(ProtocolScheduleCustomizer.class);
    final ProtocolScheduleCustomizer failedCustomizer = mock(ProtocolScheduleCustomizer.class);
    final ProtocolScheduleCustomization expected = customization("healthy");
    when(healthyCustomizer.customize(config)).thenReturn(Optional.of(expected));
    when(failedCustomizer.customize(config)).thenReturn(Optional.of(customization("failed")));

    final BesuPluginContextImpl context = registeringContext(true);
    final BesuPlugin healthy =
        plugin(
            services ->
                services
                    .getBesuService(ProtocolScheduleService.class)
                    .registerProtocolScheduleCustomizer(healthyCustomizer));
    final BesuPlugin failing =
        plugin(
            services -> {
              services
                  .getBesuService(ProtocolScheduleService.class)
                  .registerProtocolScheduleCustomizer(failedCustomizer);
              throw new IllegalStateException("a later step of registration fails");
            });

    assertThat(context.registerPlugin(healthy)).isTrue();
    assertThat(context.registerPlugin(failing)).isFalse();
    context.endRegistration();

    // only the healthy plugin's customizer is consulted, so there is no ambiguity either
    assertThat(context.resolveProtocolScheduleCustomization(config)).isSameAs(expected);
    verify(failedCustomizer, times(0)).customize(config);
  }

  @Test
  void discardingSinceAMarkKeepsEarlierRegistrations() {
    final ProtocolScheduleCustomizer earlier = mock(ProtocolScheduleCustomizer.class);
    final ProtocolScheduleCustomizer later = mock(ProtocolScheduleCustomizer.class);
    final ProtocolScheduleCustomization expected = customization("earlier");
    when(earlier.customize(config)).thenReturn(Optional.of(expected));
    when(later.customize(config)).thenReturn(Optional.of(customization("later")));
    final ProtocolScheduleServiceImpl service =
        new ProtocolScheduleServiceImpl(registrationOpen::get);
    service.registerProtocolScheduleCustomizer(earlier);
    final int mark = service.registrationMark();
    service.registerProtocolScheduleCustomizer(later);

    service.discardRegistrationsSince(mark);
    registrationOpen.set(false);

    assertThat(service.resolve(config)).isSameAs(expected);
  }

  private static final class ThrowingCustomizer implements ProtocolScheduleCustomizer {
    @Override
    public Optional<ProtocolScheduleCustomization> customize(final GenesisConfigOptions config) {
      throw new NullPointerException("optional key missing");
    }
  }

  private static BesuPlugin plugin(final Consumer<RegistrationContext> register) {
    return new BesuPlugin() {
      @Override
      public void register(final RegistrationContext context) {
        register.accept(context);
      }

      @Override
      public void start(final StartContext context) {}

      @Override
      public void stop() {}
    };
  }

  /** A context with external plugins disabled, opened for registration. */
  private static BesuPluginContextImpl registeringContext() {
    return registeringContext(false);
  }

  private static BesuPluginContextImpl registeringContext(final boolean continueOnPluginError) {
    final BesuPluginContextImpl context = new BesuPluginContextImpl();
    context.initialize(
        ImmutablePluginConfiguration.builder()
            .externalPluginsEnabled(false)
            .continueOnPluginError(continueOnPluginError)
            .build());
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
