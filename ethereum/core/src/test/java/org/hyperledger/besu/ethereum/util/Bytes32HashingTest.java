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
package org.hyperledger.besu.ethereum.util;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class Bytes32HashingTest {

  @Test
  void hashCodeKeysCollide() {
    for (int i = 0; i < 10_000; i++) {
      for (int j = 0; j < i; j++) {
        final Bytes32 key_i = Bytes32Hashing.collidingHash(i);
        final Bytes32 key_j = Bytes32Hashing.collidingHash(j);

        assertThat(key_i.hashCode()).isEqualTo(key_j.hashCode());
        assertThat(key_i).isNotEqualTo(key_j);
      }
    }
  }

  @Test
  void hashCodeKeysAreDistinct() {
    for (int i = 0; i < 10_000; i++) {
      for (int j = 0; j < i; j++) {
        final Bytes32 key_i = Bytes32Hashing.distinctHash(i);
        final Bytes32 key_j = Bytes32Hashing.distinctHash(j);

        assertThat(key_i.hashCode()).isNotEqualTo(key_j.hashCode());
        assertThat(key_i).isNotEqualTo(key_j);
      }
    }
  }
}
