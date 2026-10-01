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
package org.hyperledger.besu.ethereum.trie.immutabletree;

import org.apache.tuweni.bytes.Bytes32;

/**
 * Identifies a registered root.
 *
 * @param rootHash the root hash
 * @param kind the kind of trie
 */
public record TreeKey(Bytes32 rootHash, TreeKind kind) {

  @Override
  public String toString() {
    return kind + ":" + rootHash.toHexString();
  }
}
