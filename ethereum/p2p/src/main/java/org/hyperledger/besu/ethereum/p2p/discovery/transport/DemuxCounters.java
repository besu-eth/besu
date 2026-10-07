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
package org.hyperledger.besu.ethereum.p2p.discovery.transport;

import org.hyperledger.besu.plugin.services.metrics.Counter;

/** Bundles the three {@code discovery_demux_packets_total} label counters as a single reference. */
record DemuxCounters(Counter v4, Counter v5, Counter dropped) {

  private static final Counter NO_OP_COUNTER =
      new Counter() {
        @Override
        public void inc() {}

        @Override
        public void inc(final long amount) {}
      };

  static final DemuxCounters NO_OP = new DemuxCounters(NO_OP_COUNTER, NO_OP_COUNTER, NO_OP_COUNTER);
}
