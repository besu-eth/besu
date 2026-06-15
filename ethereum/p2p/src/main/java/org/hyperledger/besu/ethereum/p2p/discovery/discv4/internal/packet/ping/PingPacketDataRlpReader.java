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
import org.hyperledger.besu.ethereum.rlp.RLPException;
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

  private final PingPacketDataFactory pingPacketDataFactory;

  public @Inject PingPacketDataRlpReader(final PingPacketDataFactory pingPacketDataFactory) {
    this.pingPacketDataFactory = pingPacketDataFactory;
  }

  @Override
  public PingPacketData readFrom(final RLPInput in) {
    final int fieldCount = in.enterList();
    // The first element signifies the "version", but this value is ignored as of EIP-8
    in.readBigIntegerScalar();

    if (fieldCount < 3) {
      throw new DevP2PException("missing address in ping packet");
    }

    Optional<Endpoint> from;
    Optional<Endpoint> to;
    long expiration;
    if (fieldCount == 3) {
      from = Optional.empty();
      to = Endpoint.maybeDecodeStandalone(in);
      expiration = in.readLongScalar();
    } else if (in.nextIsList()) {
      final Optional<Endpoint> firstEndpoint = Endpoint.maybeDecodeStandalone(in);
      if (in.nextIsList()) {
        from = firstEndpoint;
        to = Endpoint.maybeDecodeStandalone(in);
        expiration = in.readLongScalar();
      } else {
        final RLPInput possibleExpiration = in.readAsRlp();
        try {
          // Legacy PING packets can omit the from endpoint and place expiration next.
          expiration = possibleExpiration.readLongScalar();
          from = Optional.empty();
          to = firstEndpoint;
        } catch (final RLPException invalidExpiration) {
          from = firstEndpoint;
          to = Optional.empty();
          expiration = in.readLongScalar();
        }
      }
    } else {
      from = Endpoint.maybeDecodeStandalone(in);
      to = Endpoint.maybeDecodeStandalone(in);
      expiration = in.readLongScalar();
    }

    final UInt64 enrSeq = readEnrSeq(in);
    in.leaveListLenient();
    return pingPacketDataFactory.createForDecode(from, to, expiration, enrSeq);
  }

  private UInt64 readEnrSeq(final RLPInput in) {
    if (in.isEndOfCurrentList()) {
      return null;
    }

    final RLPInput enrSeqInput = in.readAsRlp();
    try {
      final UInt64 enrSeq = UInt64.valueOf(enrSeqInput.readBigIntegerScalar());
      LOG.trace("read PING enr as long scalar");
      return enrSeq;
    } catch (final MalformedRLPInputException malformed) {
      try {
        LOG.trace("failed to read PING enr as scalar, trying to read bytes instead");
        enrSeqInput.reset();
        return UInt64.fromBytes(enrSeqInput.readBytes());
      } catch (final IllegalArgumentException | RLPException invalidEnrSeq) {
        LOG.trace("ignoring invalid PING enr sequence", invalidEnrSeq);
        return null;
      }
    } catch (final IllegalArgumentException | RLPException invalidEnrSeq) {
      LOG.trace("ignoring invalid PING enr sequence", invalidEnrSeq);
      return null;
    }
  }
}
