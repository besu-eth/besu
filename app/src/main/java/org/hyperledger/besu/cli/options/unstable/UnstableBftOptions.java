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

/** Handles unstable configuration options shared by QBFT and IBFT2 consensus engines. */
public class UnstableBftOptions {

  /** Default constructor */
  private UnstableBftOptions() {}

  /**
   * Create a new instance of UnstableBftOptions
   *
   * @return a new instance of UnstableBftOptions
   */
  public static UnstableBftOptions create() {
    return new UnstableBftOptions();
  }

  @CommandLine.Option(
      names = {"--Xbft-legacy-protocol-encoding"},
      description =
          "Enable for rolling upgrade from Besu 25.x to 26.5.0+. (default: ${DEFAULT-VALUE})",
      arity = "0..1",
      fallbackValue = "true",
      hidden = true)
  private boolean legacyProtocolEncoding = false;

  @CommandLine.Option(
      names = {"--Xbft-validate-transitions"},
      description =
          "When false, disables timestamp/block epoch validation of BFT fork transitions. "
              + "setForkType is still called for each fork. (default: ${DEFAULT-VALUE})",
      arity = "0..1",
      fallbackValue = "true",
      hidden = true)
  private boolean validateTransitions = true;

  /**
   * Whether the BFT encoder should emit the 25.x QBFT/IBFT2 wire format.
   *
   * @return true if legacy encoding is enabled
   */
  public boolean isLegacyProtocolEncodingEnabled() {
    return legacyProtocolEncoding;
  }

  /**
   * Whether BFT fork transition validation is enabled.
   *
   * @return true if transition validation is enabled
   */
  public boolean isBftValidateTransitionsEnabled() {
    return validateTransitions;
  }
}
