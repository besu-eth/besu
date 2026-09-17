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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter;

import org.hyperledger.besu.datatypes.Hash;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Tracks new blocks being added to the blockchain. */
class BlockFilter extends Filter {

  private final List<Hash> blockHashes = new ArrayList<>();

  BlockFilter(final String id, final Duration expireDuration) {
    super(id, expireDuration);
  }

  void addBlockHash(final Hash hash) {
    blockHashes.add(hash);
  }

  List<Hash> blockHashes() {
    return blockHashes;
  }

  void clearBlockHashes() {
    blockHashes.clear();
  }
}
