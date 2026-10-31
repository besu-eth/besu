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
package org.hyperledger.besu.ethereum.chain;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Objects;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Whether a block satisfied an inclusion list (EIP-7805). The same block can be sent with different
 * inclusion lists, so the status is only meaningful together with the inclusion list it was
 * computed against, identified by its checksum.
 *
 * @param inclusionListChecksum the checksum of the inclusion list the block was validated against
 * @param satisfied true if the block satisfied that inclusion list
 */
public record InclusionListStatus(Bytes32 inclusionListChecksum, boolean satisfied) {

  /**
   * Checksum of a status stored before the checksum was recorded. It never matches the checksum of
   * an actual inclusion list, so such a block is validated again.
   */
  public static final Bytes32 UNKNOWN_CHECKSUM = Bytes32.ZERO;

  private static final int ENCODED_SIZE = Bytes32.SIZE + 1;

  /**
   * Instantiates a new inclusion list status.
   *
   * @param inclusionListChecksum the checksum of the inclusion list the block was validated against
   * @param satisfied true if the block satisfied that inclusion list
   */
  public InclusionListStatus(final Bytes32 inclusionListChecksum, final boolean satisfied) {
    this.inclusionListChecksum =
        Objects.requireNonNull(inclusionListChecksum, "inclusionListChecksum must not be null");
    this.satisfied = satisfied;
  }

  /**
   * Whether this status was computed against the inclusion list with the given checksum.
   *
   * @param checksum the checksum of an inclusion list
   * @return true if this status is for that inclusion list
   */
  public boolean isFor(final Bytes32 checksum) {
    return inclusionListChecksum.equals(checksum);
  }

  /**
   * Encodes this status for storage, as the checksum followed by one byte for the satisfied flag.
   *
   * @return the encoded status
   */
  public Bytes encode() {
    return Bytes.concatenate(inclusionListChecksum, Bytes.of(satisfied ? 1 : 0));
  }

  /**
   * Decodes a stored status. A single byte is a status stored before the checksum was recorded.
   *
   * @param encoded the encoded status
   * @return the decoded status
   */
  public static InclusionListStatus decode(final Bytes encoded) {
    if (encoded.size() == 1) {
      return new InclusionListStatus(UNKNOWN_CHECKSUM, encoded.get(0) != 0);
    }
    checkArgument(
        encoded.size() == ENCODED_SIZE,
        "Invalid encoded inclusion list status size %s",
        encoded.size());
    return new InclusionListStatus(
        Bytes32.wrap(encoded.slice(0, Bytes32.SIZE)), encoded.get(Bytes32.SIZE) != 0);
  }
}
