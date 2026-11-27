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
package org.hyperledger.besu.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

class KeyedRegistrationsTest {
  private final AtomicReference<String> registrant = new AtomicReference<>("pluginA");
  private final KeyedRegistrations<String> registrations =
      new KeyedRegistrations<>("thing", () -> Optional.ofNullable(registrant.get()));

  @Test
  void aNameAnotherPluginRegisteredIsRejectedNamingBoth() {
    registrations.register("shared", "a");
    registrant.set("pluginB");

    assertThatThrownBy(() -> registrations.register("shared", "b"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("cannot register thing shared: plugin pluginA already registered it");
    assertThat(registrations.get("shared")).contains("a");
  }

  @Test
  void aPluginMayRegisterItsOwnNameAgain() {
    registrations.register("own", "first");

    registrations.register("own", "second");

    assertThat(registrations.get("own")).contains("second");
  }

  @Test
  void aNameBesuRegisteredIsRejectedNamingBesu() {
    registrations.registerAsBesu("default", "besu");

    assertThatThrownBy(() -> registrations.register("default", "mine"))
        .hasMessage("cannot register thing default: Besu already registered it");
  }

  @Test
  void besuRegistersWhenNoPluginIsRegistering() {
    registrant.set(null);
    registrations.register("default", "besu");
    registrant.set("pluginA");

    assertThatThrownBy(() -> registrations.register("default", "mine"))
        .hasMessage("cannot register thing default: Besu already registered it");
    assertThat(registrations.currentOwner()).isEqualTo("plugin pluginA");
  }

  @Test
  void replaceReportsThePreviousOwnerOnlyWhenItIsAnotherRegistrant() {
    assertThat(registrations.replace("check", "a")).isEmpty();
    assertThat(registrations.replace("check", "a2")).isEmpty();
    registrant.set("pluginB");

    assertThat(registrations.replace("check", "b")).contains("plugin pluginA");
    assertThat(registrations.get("check")).contains("b");
  }

  @Test
  void namesAreNormalizedForStorageAndLookupButPrintedAsGiven() {
    final KeyedRegistrations<String> upperCased =
        new KeyedRegistrations<>(
            "category", () -> Optional.of("pluginA"), name -> name.toUpperCase(Locale.ROOT));
    upperCased.registerAsBesu("PROCESS", "besu");

    assertThatThrownBy(() -> upperCased.register("process", "mine"))
        .hasMessage("cannot register category process: Besu already registered it");
    assertThat(upperCased.contains("Process")).isTrue();
    assertThat(upperCased.get("process")).contains("besu");
    assertThat(upperCased.names()).containsExactly("PROCESS");
  }

  @Test
  void entriesCanBeRemovedAndViewedByName() {
    registrations.register("one", "1");
    registrations.register("two", "2");

    registrations.remove("one");

    assertThat(registrations.contains("one")).isFalse();
    assertThat(registrations.byName()).containsOnlyKeys("two").containsEntry("two", "2");
    assertThatThrownBy(() -> registrations.byName().put("three", "3"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void theRegistrantOfANameIsReported() {
    registrations.register("mine", "a");
    registrant.set(null);
    registrations.register("besus", "b");

    assertThat(registrations.registrantOf("mine")).contains("pluginA");
    assertThat(registrations.registrantOf("besus")).contains(KeyedRegistrations.BESU);
    assertThat(registrations.registrantOf("unknown")).isEmpty();
  }
}
