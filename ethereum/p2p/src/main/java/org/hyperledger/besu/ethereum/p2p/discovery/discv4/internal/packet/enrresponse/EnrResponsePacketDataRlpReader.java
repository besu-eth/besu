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
package org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.enrresponse;

import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryPacketDecodingException;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketDataDeserializer;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.rlp.RLPException;
import org.ethereum.beacon.discovery.schema.NodeRecord;
import org.ethereum.beacon.discovery.schema.NodeRecordFactory;
import org.ethereum.beacon.discovery.util.DecodeException;

@Singleton
public class EnrResponsePacketDataRlpReader
    implements PacketDataDeserializer<EnrResponsePacketData> {

  private final NodeRecordFactory nodeRecordFactory;
  private final EnrResponsePacketDataFactory enrResponsePacketDataFactory;

  public @Inject EnrResponsePacketDataRlpReader(
      final NodeRecordFactory nodeRecordFactory,
      final EnrResponsePacketDataFactory enrResponsePacketDataFactory) {
    this.nodeRecordFactory = nodeRecordFactory;
    this.enrResponsePacketDataFactory = enrResponsePacketDataFactory;
  }

  @Override
  public EnrResponsePacketData readFrom(final RLPInput in) {
    in.enterList();
    final Bytes requestHash = in.readBytes();
    in.leaveListLenient();
    try {
      final NodeRecord enr = nodeRecordFactory.fromBytes(in.currentListAsBytes());
      return enrResponsePacketDataFactory.create(requestHash, enr);
    } catch (final RLPException | DecodeException | IllegalArgumentException e) {
      throw new PeerDiscoveryPacketDecodingException("Invalid ENR in ENR_RESPONSE packet", e);
    }
  }
}
