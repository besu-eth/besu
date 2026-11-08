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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import org.hyperledger.besu.ethereum.core.BlockHeader;

import org.apache.tuweni.bytes.Bytes32;

/**
 * A block whose state is downloaded. Each move to a newer pivot starts a new epoch, and responses
 * to requests sent at an older epoch are dropped.
 *
 * @param epoch the number of moves before this pivot
 * @param header the header of the pivot
 */
public record Pivot(int epoch, BlockHeader header) {

  /** Returns the state root of the pivot. */
  Bytes32 root() {
    return Bytes32.wrap(header.getStateRoot().getBytes());
  }
}
