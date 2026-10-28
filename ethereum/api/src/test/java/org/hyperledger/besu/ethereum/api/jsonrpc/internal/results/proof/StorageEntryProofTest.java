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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.proof;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class StorageEntryProofTest {

  @Test
  void returnsKeyAsGiven() {
    assertThat(new StorageEntryProof("0x0", UInt256.ZERO, List.of()).getKey()).isEqualTo("0x0");
    assertThat(new StorageEntryProof("0x1", UInt256.ZERO, List.of()).getKey()).isEqualTo("0x1");
    final String fullKey = "0x0000000000000000000000000000000000000000000000000000000000000347";
    assertThat(new StorageEntryProof(fullKey, UInt256.ZERO, List.of()).getKey()).isEqualTo(fullKey);
  }

  @Test
  void returnsValueAsQuantityAndProofAsHex() {
    final StorageEntryProof entry =
        new StorageEntryProof("0x1", UInt256.valueOf(0x10), List.of(Bytes.fromHexString("0xab")));
    assertThat(entry.getValue()).isEqualTo("0x10");
    assertThat(entry.getStorageProof()).containsExactly("0xab");
  }
}
