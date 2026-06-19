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
import org.apache.tuweni.bytes.Bytes48;

/** This class contains the data for a KZG proof for a KZG commitment. */
public class KZGProof implements org.hyperledger.besu.datatypes.KZGProof {
  static int SIZE = 48;
  private final Bytes48 data;

  /**
   * Constructor for a KZG proof.
   *
   * @param data The data for the KZG proof.
   */
  @JsonCreator
  public KZGProof(final Bytes48 data) {
    this.data = data;
  }

  /**
   * Reads a KZG proof from the RLP input.
   *
   * @param input The RLP input.
   * @return The KZG proof.
   */
  public static KZGProof readFrom(final RLPInput input) {
    final Bytes48 bytes = input.readBytes48();
    return new KZGProof(bytes);
  }

  /**
   * Writes the KZG proof to the RLP output.
   *
   * @param out The RLP output.
   */
  public void writeTo(final RLPOutput out) {
    out.writeBytes(data);
  }

  /**
   * Gets the data for the KZG proof.
   *
   * @return The data for the KZG proof.
   */
  @Override
  @JsonValue
  public Bytes48 getData() {
    return data;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KZGProof kzgProof = (KZGProof) o;
    return Objects.equals(getData(), kzgProof.getData());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getData());
  }
}
