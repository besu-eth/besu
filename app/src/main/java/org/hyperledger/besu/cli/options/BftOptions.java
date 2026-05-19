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

/** Handles configuration options shared by QBFT and IBFT2 consensus engines. */
public class BftOptions {

  /** Default constructor */
  private BftOptions() {}

  /**
   * Create a new instance of BftOptions
   *
   * @return a new instance of BftOptions
   */
  public static BftOptions create() {
    return new BftOptions();
  }

  @CommandLine.Option(
      names = {"--Xbft-legacy-protocol-encoding"},
      description =
          "Use the 25.x QBFT/IBFT2 protocol encoding format. Set to true only when "
              + "rolling-upgrading from Besu 25.x peers. Has no effect on chains where "
              + "blockAccessList is active. Experimental - will be removed once Besu 25.x "
              + "is no longer supported. (default: ${DEFAULT-VALUE})",
      arity = "1",
      hidden = true)
  private boolean legacyProtocolEncoding = false;

  /**
   * Whether the BFT encoder should emit the 25.x QBFT/IBFT2 wire format.
   *
   * @return true if legacy encoding is enabled
   */
  public boolean isLegacyProtocolEncodingEnabled() {
    return legacyProtocolEncoding;
  }
}
