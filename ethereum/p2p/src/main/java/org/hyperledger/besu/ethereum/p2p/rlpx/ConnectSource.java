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
package org.hyperledger.besu.ethereum.p2p.rlpx;

import java.util.Locale;

/**
 * Identifies the originating source of an outbound RLPx connection attempt, used as a label value
 * for the {@code rlpx_outbound_connect_attempts_total} metric.
 */
public enum ConnectSource {
  /** Peer added via {@code admin_addPeer} or an explicit maintained-peer call. */
  ADMIN,
  /** Peer selected from the maintained-peer pool or the discovered-peer pool in DefaultP2PNetwork. */
  MAINTAIN,
  /** Peer discovered and promoted by the DiscV4 bonding pipeline. */
  DISCV4,
  /** Peer discovered and promoted by the DiscV5 candidate pipeline. */
  DISCV5;

  /** Returns the lowercase Prometheus label value for this source. */
  public String label() {
    return name().toLowerCase(Locale.ROOT);
  }
}
