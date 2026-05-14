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
package org.hyperledger.besu.cli.options.unstable;

import picocli.CommandLine;

/** Handles configuration options for QBFT consensus */
public class QBFTOptions {

  /** Default constructor */
  private QBFTOptions() {}

  /**
   * Create a new instance of QBFTOptions
   *
   * @return a new instance of QBFTOptions
   */
  public static QBFTOptions create() {
    return new QBFTOptions();
  }

  @CommandLine.Option(
      names = {"--Xqbft-enable-early-round-change"},
      description =
          "Enable early round change upon receiving f+1 valid future Round Change messages from different validators (experimental)",
      hidden = true)
  private boolean enableEarlyRoundChange = false;

  @CommandLine.Option(
      names = {"--Xqbft-legacy-roundchange-encoding"},
      description =
          "Emit the pre-26.1.0 QBFT RoundChange and Proposal wire format (omits the "
              + "blockAccessList field when absent). Set to true only when rolling-upgrading "
              + "from Besu 25.x peers; this format cannot be decoded by Besu 26.1.0-26.5.0 "
              + "peers. The flag is experimental and will be removed once Besu 25.x is no "
              + "longer supported. (default: ${DEFAULT-VALUE})",
      arity = "1",
      hidden = true)
  private boolean legacyRoundChangeEncoding = false;

  /**
   * Is early round change enabled boolean.
   *
   * @return true if early round change is enabled
   */
  public boolean isEarlyRoundChangeEnabled() {
    return enableEarlyRoundChange;
  }

  /**
   * Whether the encoder should emit the pre-26.1.0 RoundChange / Proposal wire format.
   *
   * @return true if legacy encoding is enabled
   */
  public boolean isLegacyRoundChangeEncodingEnabled() {
    return legacyRoundChangeEncoding;
  }
}
