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
package org.hyperledger.besu.cli.options;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;

import java.util.Arrays;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;

public class LoggingLevelOptionTest {

  private LoggingLevelOption levelOption;

  @BeforeEach
  public void setUp() {
    levelOption = LoggingLevelOption.create();
  }

  @Test
  public void fatalLevelEqualsToError() {
    levelOption.setLogLevel("fatal");
    assertThat(levelOption.getLogLevel()).isEqualTo("ERROR");
  }

  @Test
  public void setsExpectedLevels() {
    Arrays.stream(Level.values())
        .filter(level -> !Level.FATAL.equals(level))
        .forEach(
            level -> {
              levelOption.setLogLevel(level.name());
              assertThat(levelOption.getLogLevel()).isEqualTo(level.name());
            });
  }

  @Test
  public void failsOnUnknownLevel() {
    levelOption.spec = Mockito.mock(CommandSpec.class, RETURNS_DEEP_STUBS);
    assertThatThrownBy(() -> levelOption.setLogLevel("unknown"))
        .isInstanceOf(ParameterException.class);
  }

  @Test
  public void defaultLoggingFormatIsPlain() {
    assertThat(levelOption.getLoggingFormat()).isEqualTo(LoggingFormat.PLAIN);
  }

  @Test
  public void setsExpectedLoggingFormats() {
    Arrays.stream(LoggingFormat.values())
        .forEach(
            format -> {
              levelOption.setLoggingFormat(format);
              assertThat(levelOption.getLoggingFormat()).isEqualTo(format);
            });
  }
}
