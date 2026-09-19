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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.code;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.Code;

import org.apache.tuweni.bytes.Bytes;

/**
 * Code as it comes out of storage, together with its jump destination analysis when storage has it.
 *
 * @param code the code bytes
 * @param jumpDestBitMask the jump destination bitmask of the code, or null if it has to be computed
 */
public record StoredCode(Bytes code, long[] jumpDestBitMask) {

  public static StoredCode withoutAnalysis(final Bytes code) {
    return new StoredCode(code, null);
  }

  public Code toCode(final Hash codeHash) {
    if (jumpDestBitMask == null) {
      return new Code(code, codeHash);
    }
    // The analysis being present is what lets the storage value be kept as it is, without a copy
    return new Code(code, codeHash, jumpDestBitMask);
  }
}
