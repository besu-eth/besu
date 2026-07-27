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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.ping;

import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class PingPacketDataRlpWriter {

  public @Inject PingPacketDataRlpWriter() {}

  public void writeTo(final PingPacketData pingPacketData, final RLPOutput out) {
    out.startList();
    out.writeIntScalar(PingPacketData.VERSION);
    if (pingPacketData.getFrom().isPresent()) {
      pingPacketData.getFrom().get().encodeStandalone(out);
    }
    pingPacketData.getTo().orElseThrow().encodeStandalone(out);
    out.writeLongScalar(pingPacketData.getExpiration());
    out.writeBigIntegerScalar(
        pingPacketData.getEnrSeq().map(seq -> seq.toBigInteger()).orElseThrow());
    out.endList();
  }
}
