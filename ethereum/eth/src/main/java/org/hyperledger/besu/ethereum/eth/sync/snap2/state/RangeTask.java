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

import org.apache.tuweni.bytes.Bytes32;

/**
 * A key range downloaded with consecutive requests, each one resuming after the previous. Not
 * thread safe: the loop of the state download owns it.
 */
final class RangeTask {
  final Bytes32 limit;
  Bytes32 next;
  boolean inFlight;
  boolean done;

  RangeTask(final Bytes32 origin, final Bytes32 limit) {
    this.limit = limit;
    this.next = origin;
  }
}
