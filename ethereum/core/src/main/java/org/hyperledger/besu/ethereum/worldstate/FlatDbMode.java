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
package org.hyperledger.besu.ethereum.worldstate;

import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;

/**
 * The FlatDbMode enum represents the different modes of the flat database. It has two modes:
 * PARTIAL and FULL.
 *
 * <p>- PARTIAL: Not all the leaves are present inside the flat database. The trie serves as a
 * fallback to retrieve missing data. The PARTIAL mode is primarily used for backward compatibility
 * purposes, where the flat database may not have all the required data, and the trie is utilized to
 * fill in the gaps.
 *
 * <p>- FULL: The flat database contains the complete representation of the world state, and there
 * is no need for a fallback mechanism. The FULL mode represents a fully synchronized state where
 * the flat database encompasses all the necessary data.
 */
public enum FlatDbMode {
  NO_FLATTENED(Bytes.EMPTY),
  PARTIAL(Bytes.of(0x00)),
  FULL(Bytes.of(0x01));

  final Bytes version;

  FlatDbMode(final Bytes version) {
    this.version = version;
  }

  public Bytes getVersion() {
    return version;
  }

  public static FlatDbMode fromVersion(final Bytes version) {
    return Stream.of(FlatDbMode.values())
        .filter(mode -> mode.getVersion().equals(version))
        .findFirst()
        .orElseThrow(
            () -> new IllegalArgumentException("Unknown flat DB mode version: " + version));
  }
}
