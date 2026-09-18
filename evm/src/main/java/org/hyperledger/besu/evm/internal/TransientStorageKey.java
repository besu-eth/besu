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
package org.hyperledger.besu.evm.internal;

import org.hyperledger.besu.datatypes.Address;

import java.util.Arrays;
import java.util.Objects;

import org.apache.tuweni.bytes.Bytes32;

/**
 * Key to use for lookups in Maps/Sets with an address and storage slot combined as a key.
 *
 * @param address the address part of the key
 * @param slot the slot part of the key
 */
public record TransientStorageKey(Address address, Bytes32 slot)
    implements Comparable<TransientStorageKey> {
  @Override
  public int hashCode() {
    int initialValue = Arrays.hashCode(address.getBytes().toArrayUnsafe());
    return 31 * initialValue + Arrays.hashCode(slot.toArrayUnsafe());
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof TransientStorageKey other) {
      return Objects.equals(address, other.address) && Objects.equals(slot, other.slot);
    }
    return false;
  }

  @Override
  public int compareTo(final TransientStorageKey other) {
    if (this == other) {
      return 0;
    }
    if (other == null) {
      return 1;
    }
    int compare =
        Arrays.compare(
            address.getBytes().toArrayUnsafe(), other.address.getBytes().toArrayUnsafe());
    return compare != 0
        ? compare
        : Arrays.compare(slot.toArrayUnsafe(), other.slot.toArrayUnsafe());
  }
}
