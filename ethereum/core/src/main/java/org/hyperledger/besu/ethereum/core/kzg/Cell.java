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
package org.hyperledger.besu.ethereum.core.kzg;

import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import org.apache.tuweni.bytes.Bytes;

public class Cell {
  public static final int SIZE = 2048;

  private final Bytes data;

  /**
   * Create a new Cell.
   *
   * @param data that represents the blob.
   */
  @JsonCreator
  public Cell(final Bytes data) {
    Preconditions.checkArgument(
        data.size() == SIZE, "Invalid cell size %d, expected %d".formatted(data.size(), SIZE));
    this.data = data;
  }

  /**
   * Read a Cell from an RLPInput.
   *
   * @param input to read from.
   * @return the Cell.
   * @throws RLPException if the encoded cell does not have the expected size. Peer supplied data
   *     reaches this method, so a malformed size is reported as an RLP error rather than as an
   *     unchecked argument error.
   */
  public static Cell readFrom(final RLPInput input) {
    final Bytes bytes = input.readBytes();
    if (bytes.size() != SIZE) {
      throw new RLPException("Invalid cell size %d, expected %d".formatted(bytes.size(), SIZE));
    }
    return new Cell(bytes);
  }

  /**
   * Write the Cell to an RLPOutput.
   *
   * @param out to write to.
   */
  public void writeTo(final RLPOutput out) {
    out.writeBytes(data);
  }

  /**
   * Get the data of the Cell.
   *
   * @return the data.
   */
  @JsonValue
  public Bytes getData() {
    return data;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Cell cell = (Cell) o;
    return Objects.equals(getData(), cell.getData());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getData());
  }
}
