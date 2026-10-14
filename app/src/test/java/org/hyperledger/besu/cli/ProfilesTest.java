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
package org.hyperledger.besu.cli;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.cli.config.InternalProfileName;
import org.hyperledger.besu.cli.util.ProfileFinder;
import org.hyperledger.besu.services.BesuPluginContextImpl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Spy;

public class ProfilesTest extends CommandTestAbstract {

  @Spy private BesuPluginContextImpl besuPluginContextSpy;

  @Override
  protected BesuPluginContextImpl getBesuPluginContext() {
    return besuPluginContextSpy;
  }

  @TempDir private static Path tempProfilesDir;
  private static String originalProfilesDirProperty;

  @BeforeAll
  public static void copyExternalProfiles() throws IOException {
    for (String internalProfileName : InternalProfileName.getInternalProfileNames()) {
      final Path profilePath = tempProfilesDir.resolve(internalProfileName + "_external.toml");

      String profileConfigFile =
          InternalProfileName.valueOfIgnoreCase(internalProfileName).get().getConfigFile();
      try (InputStream resourceUrl =
          ProfileFinder.class.getClassLoader().getResourceAsStream(profileConfigFile)) {
        if (resourceUrl != null) {
          Files.copy(resourceUrl, profilePath);
        }
      }
    }

    // add an empty external profile
    Files.createFile(tempProfilesDir.resolve("empty_external.toml"));
  }

  @BeforeAll
  public static void setupSystemProperty() {
    originalProfilesDirProperty = System.getProperty("besu.profiles.dir");
    // sets the system property for the test
    System.setProperty("besu.profiles.dir", tempProfilesDir.toString());
  }

  static Stream<Arguments> profileNameProvider() {
    final Set<String> profileNames = new TreeSet<>(InternalProfileName.getInternalProfileNames());
    final Set<String> externalProfileNames =
        InternalProfileName.getInternalProfileNames().stream()
            .map(name -> name + "_external")
            .collect(Collectors.toSet());
    profileNames.addAll(externalProfileNames);
    return profileNames.stream().map(Arguments::of);
  }

  /** Test if besu will validate the combination of options within the given profile. */
  @ParameterizedTest(name = "{index} - Profile Name override: {0}")
  @DisplayName("Valid Profile with overrides does not error")
  @MethodSource("profileNameProvider")
  public void testProfileWithNoOverrides_doesNotError(final String profileName) {
    parseCommand("--profile", profileName);

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  @DisplayName("Empty external profile file results in error")
  public void emptyProfileFile_ShouldResultInError() {
    parseCommand("--profile", "empty_external");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Unable to read from empty TOML configuration file.");
  }

  @Test
  @DisplayName("Non Existing profile results in error")
  public void nonExistentProfileFile_ShouldResultInError() {
    parseCommand("--profile", "non_existent_profile");

    assertThat(commandOutput.toString(UTF_8)).isEmpty();
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("Unable to load external profile: non_existent_profile");
  }

  @AfterAll
  public static void clearSystemProperty() {
    if (originalProfilesDirProperty != null) {
      System.setProperty("besu.profiles.dir", originalProfilesDirProperty);
    } else {
      System.clearProperty("besu.profiles.dir");
    }
  }
}
