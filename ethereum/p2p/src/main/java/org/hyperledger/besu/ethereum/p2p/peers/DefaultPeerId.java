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
package org.hyperledger.besu.ethereum.p2p.peers;

import org.hyperledger.besu.crypto.Hash;

import java.util.Objects;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class DefaultPeerId implements PeerId {
  protected final Bytes id;
  private Bytes32 keccak256;

  public DefaultPeerId(final Bytes id) {
    this.id = id;
  }

  @Override
  public Bytes getId() {
    return id;
  }

  @Override
  public String getLoggableId() {
    return getId().slice(0, 8) + "...";
  }

  @Override
  public Bytes32 keccak256() {
    if (keccak256 == null) {
      keccak256 = Hash.keccak256(getId());
    }
    return keccak256;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (!(o instanceof DefaultPeerId that)) return false;
    return Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
