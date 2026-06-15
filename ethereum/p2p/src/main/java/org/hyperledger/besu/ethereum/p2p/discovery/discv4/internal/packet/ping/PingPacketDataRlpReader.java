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

import org.hyperledger.besu.ethereum.p2p.discovery.discv4.Endpoint;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.DevP2PException;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketDataDeserializer;
import org.hyperledger.besu.ethereum.rlp.MalformedRLPInputException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.tuweni.units.bigints.UInt64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class PingPacketDataRlpReader implements PacketDataDeserializer<PingPacketData> {
  private static final Logger LOG = LoggerFactory.getLogger(PingPacketDataRlpReader.class);

  public @Inject PingPacketDataRlpReader() {}

  @Override
  public PingPacketData readFrom(final RLPInput in) {
    in.enterList();
    // The first element signifies the "version", but this value is ignored as of EIP-8
    in.readBigIntegerScalar();
    Optional<Endpoint> from = Optional.empty();
    Optional<Endpoint> to = Optional.empty();
    if (in.nextIsList()) {
      to = Endpoint.maybeDecodeStandalone(in);
      // https://github.com/ethereum/devp2p/blob/master/discv4.md#ping-packet-0x01
      if (in.nextIsList()) { // if there are two, the first is the from address, next is the to
        // address
        from = to;
        to = Endpoint.maybeDecodeStandalone(in);
      }
    } else {
      throw new DevP2PException("missing address in ping packet");
    }
    final long expiration = in.readLongScalar();
    UInt64 enrSeq = null;
    if (!in.isEndOfCurrentList()) {
      try {
        enrSeq = UInt64.valueOf(in.readBigIntegerScalar());
        LOG.trace("read PING enr as long scalar");
      } catch (MalformedRLPInputException malformed) {
        LOG.trace("failed to read PING enr as scalar, trying to read bytes instead");
        enrSeq = UInt64.fromBytes(in.readBytes());
      }
    }
    in.leaveListLenient();
    // Per EIP-8, a malformed to field must not prevent packet processing.
    // The to field is unused when constructing our PONG; we always respond to the actual sender.
    return new PingPacketData(from, to, expiration, enrSeq);
  }
}
