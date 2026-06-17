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
package org.hyperledger.besu.ethereum.trie.pathbased.common.code;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.Code;

class CodeMemoryFootprint {
  public static int estimate(final Hash key, final Code code) {
    // Estimate the memory usage of the Code object based on:
    // - code.getSize(): the number of bytes of the contract code
    // - key.size(): the size of the hash key (typically 32 bytes for Keccak-256)

    // The formula ((code.getSize() * 9 + 7) / 8) breaks down as:
    // 1. code.getSize() * 9:
    //    - Each byte of code contributes:
    //      - 1 byte for the actual bytecode
    //      - ~1 bit for the jumpdest bitmap (1 bit per byte)
    //      - ~small overhead per byte for metadata/indexing (approximated)
    //    - So 9 bits per byte is a rough overestimate to account for that

    // 2. +7 and /8:
    //    - This effectively rounds up to the nearest full byte
    //    - It's a common trick to round up a bit count to byte count: (bits + 7) / 8

    // 3. + key.size():
    //    - Adds the memory used by the hash key (usually 32 bytes)
    return ((code.getSize() * 9 + 7) / 8) + key.getBytes().size();
  }
}
