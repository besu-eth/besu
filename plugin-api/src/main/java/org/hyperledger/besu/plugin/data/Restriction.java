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
package org.hyperledger.besu.plugin.data;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.tuweni.bytes.Bytes;

/**
 * The enum Restriction.
 *
 * @deprecated This type is scheduled for removal in a future release, with no replacement. It
 *     belonged to the private transaction API removed in Besu 25.6.0 and is no longer reachable
 *     from any plugin contract.
 */
@Deprecated(forRemoval = true)
public enum Restriction {
  /** Restricted restriction. */
  RESTRICTED(Bytes.wrap("restricted".getBytes(UTF_8))),
  /** Unrestricted restriction. */
  UNRESTRICTED(Bytes.wrap("unrestricted".getBytes(UTF_8))),
  /** Unsupported restriction. */
  UNSUPPORTED(Bytes.EMPTY);

  private final Bytes bytes;

  Restriction(final Bytes bytes) {
    this.bytes = bytes;
  }

  /**
   * Gets bytes.
   *
   * @return the bytes
   */
  public Bytes getBytes() {
    return bytes;
  }
}
