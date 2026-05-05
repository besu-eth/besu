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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.ethereum.rlp.RLP;

import java.util.Objects;

import org.apache.tuweni.bytes.Bytes;

public class SyncBlockAccessList {

  private final Bytes rlp;

  public SyncBlockAccessList(final Bytes rlp) {
    this.rlp = Objects.requireNonNull(rlp);
  }

  public Bytes getRlp() {
    return rlp;
  }

  public boolean isEmpty() {
    return rlp.equals(RLP.EMPTY_LIST);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof SyncBlockAccessList)) {
      return false;
    }
    final SyncBlockAccessList other = (SyncBlockAccessList) obj;
    return rlp.equals(other.rlp);
  }

  @Override
  public int hashCode() {
    return rlp.hashCode();
  }

  @Override
  public String toString() {
    return "SyncBlockAccessList{" + "rlp=" + rlp + '}';
  }
}
