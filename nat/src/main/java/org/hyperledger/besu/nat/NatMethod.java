/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.nat;

/** The enum Nat method. */
public enum NatMethod {
  /** Upnp nat method. */
  UPNP,
  /** Upnp p2p only nat method. */
  UPNPP2PONLY,
  /** Docker nat method. */
  DOCKER,
  /** Auto nat method. */
  AUTO,
  /** None nat method. */
  NONE;

  /**
   * Map NatMethod from string value.
   *
   * @param str the Nat Method in String format
   * @return instance of mapped NatMethod, or {@link NatMethod#NONE} if no match is found
   */
  public static NatMethod fromString(final String str) {
    for (final NatMethod mode : NatMethod.values()) {
      if (mode.name().equalsIgnoreCase(str)) {
        return mode;
      }
    }
    return NONE;
  }
}
