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
package org.hyperledger.besu.ethereum.core.kzg;

import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.tuweni.bytes.Bytes;

/** Arbitrary data for use in the KZG scheme. */
public class Blob implements org.hyperledger.besu.datatypes.Blob {

  private final Bytes data;

  /**
   * Create a new Blob.
   *
   * @param data that represents the blob.
   */
  @JsonCreator
  public Blob(final Bytes data) {
    this.data = data;
  }

  /**
   * Read a Blob from an RLPInput.
   *
   * @param input to read from.
   * @return the Blob.
   */
  public static Blob readFrom(final RLPInput input) {
    final Bytes bytes = input.readBytes();
    return new Blob(bytes);
  }

  /**
   * Write the Blob to an RLPOutput.
   *
   * @param out to write to.
   */
  public void writeTo(final RLPOutput out) {
    out.writeBytes(data);
  }

  /**
   * Get the data of the Blob.
   *
   * @return the data.
   */
  @Override
  @JsonValue
  public Bytes getData() {
    return data;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Blob blob = (Blob) o;
    return Objects.equals(getData(), blob.getData());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getData());
  }
}
