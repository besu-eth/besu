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
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.PacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.EndpointValidator;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.ExpiryValidator;

import java.time.Clock;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.tuweni.units.bigints.UInt64;

@Singleton
public class PingPacketDataFactory {
  private final EndpointValidator endpointValidator;
  private final ExpiryValidator expiryValidator;
  private final Clock clock;

  public @Inject PingPacketDataFactory(
      final EndpointValidator endpointValidator,
      final ExpiryValidator expiryValidator,
      final Clock clock) {
    this.endpointValidator = endpointValidator;
    this.expiryValidator = expiryValidator;
    this.clock = clock;
  }

  public PingPacketData create(
      final Optional<Endpoint> maybeFrom,
      final Endpoint to,
      final long expiration,
      final UInt64 enrSeq) {
    endpointValidator.validate(to, "destination endpoint cannot be null");
    expiryValidator.validate(expiration);
    return new PingPacketData(maybeFrom, Optional.of(to), expiration, enrSeq);
  }

  public PingPacketData create(
      final Optional<Endpoint> maybeFrom, final Endpoint to, final UInt64 enrSeq) {
    endpointValidator.validate(to, "destination endpoint cannot be null");
    return new PingPacketData(maybeFrom, Optional.of(to), getDefaultExpirationTime(), enrSeq);
  }

  private long getDefaultExpirationTime() {
    return clock.instant().getEpochSecond() + PacketData.DEFAULT_EXPIRATION_PERIOD_SEC;
  }
}
