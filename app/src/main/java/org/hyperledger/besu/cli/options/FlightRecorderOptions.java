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
package org.hyperledger.besu.cli.options;

import org.hyperledger.besu.cli.util.FlightRecorderStartup;

import java.nio.file.Path;

import picocli.CommandLine;

/**
 * Hidden opt-in for a continuous Java Flight Recording under the data directory.
 *
 * <p>The default launcher used to pass {@code -XX:StartFlightRecording}, which stores the recording
 * in the temporary directory. The JVM removes that directory only on a clean shutdown, so a killed
 * process leaves it behind.
 */
public class FlightRecorderOptions {

  /** CLI flag. Also {@code BESU_XFLIGHT_RECORDER}, via the environment default provider. */
  public static final String FLIGHT_RECORDER = "--Xflight-recorder";

  /** Default constructor. */
  public FlightRecorderOptions() {}

  @CommandLine.Option(
      names = {FLIGHT_RECORDER},
      description =
          "Start a continuous Java Flight Recording named 'besu' under <data-path>/jfr and delete"
              + " recordings left there by killed runs (default: ${DEFAULT-VALUE}). Dump with:"
              + " jcmd <pid> JFR.dump name=besu filename=besu.jfr. Also BESU_XFLIGHT_RECORDER.",
      arity = "1",
      hidden = true)
  private boolean enabled = false;

  /**
   * Whether the operator asked for a continuous recording.
   *
   * @return {@code true} when {@code --Xflight-recorder} or {@code BESU_XFLIGHT_RECORDER} is set
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Starts the recording when this option is enabled. Does nothing when it is disabled, including
   * leaving any existing files in place.
   *
   * @param dataDirectory Besu data directory. The repository is {@code dataDirectory/jfr}.
   */
  public void startRecording(final Path dataDirectory) {
    FlightRecorderStartup.startIfEnabled(enabled, dataDirectory);
  }
}
