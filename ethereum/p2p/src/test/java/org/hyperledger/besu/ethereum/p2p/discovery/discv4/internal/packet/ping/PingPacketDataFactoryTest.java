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
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.EndpointValidator;
import org.hyperledger.besu.ethereum.p2p.discovery.discv4.internal.packet.validation.ExpiryValidator;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;

import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PingPacketDataFactoryTest {
  private @Mock EndpointValidator endpointValidator;
  private @Mock ExpiryValidator expiryValidator;
  private final Clock clock = Clock.fixed(Instant.ofEpochSecond(123), ZoneId.of("UTC"));

  private PingPacketDataFactory factory;

  @BeforeEach
  public void beforeTest() {
    factory = new PingPacketDataFactory(endpointValidator, expiryValidator, clock);
  }

  @Test
  public void testCreateWithExpiry() {
    final Optional<Endpoint> maybeFrom = Optional.empty();
    final Endpoint to = Mockito.mock(Endpoint.class);
    final long expiration = 456;
    final UInt64 enrSeq = UInt64.MAX_VALUE;

    PingPacketData result = factory.create(maybeFrom, to, expiration, enrSeq);

    Mockito.verify(endpointValidator).validate(to, "destination endpoint cannot be null");
    Mockito.verify(expiryValidator).validate(expiration);

    Assertions.assertEquals(maybeFrom, result.getFrom());
    Assertions.assertEquals(Optional.of(to), result.getTo());
    Assertions.assertEquals(expiration, result.getExpiration());
    Assertions.assertTrue(result.getEnrSeq().isPresent());
    Assertions.assertEquals(enrSeq, result.getEnrSeq().get());
  }

  @Test
  public void testCreateWithExpiryWithInvalidToAddress() {
    final Optional<Endpoint> maybeFrom = Optional.empty();
    final Endpoint to = Mockito.mock(Endpoint.class);
    final long expiration = 456;
    final UInt64 enrSeq = UInt64.MAX_VALUE;

    Mockito.doThrow(new IllegalArgumentException())
        .when(endpointValidator)
        .validate(to, "destination endpoint cannot be null");

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> factory.create(maybeFrom, to, expiration, enrSeq));

    Mockito.verify(endpointValidator).validate(to, "destination endpoint cannot be null");
  }

  @Test
  public void testCreateWithExpiryWithInvalidExpiration() {
    final Optional<Endpoint> maybeFrom = Optional.empty();
    final Endpoint to = Mockito.mock(Endpoint.class);
    final long expiration = 456;
    final UInt64 enrSeq = UInt64.MAX_VALUE;

    Mockito.doThrow(new IllegalArgumentException()).when(expiryValidator).validate(expiration);

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> factory.create(maybeFrom, to, expiration, enrSeq));

    Mockito.verify(endpointValidator).validate(to, "destination endpoint cannot be null");
    Mockito.verify(expiryValidator).validate(expiration);
  }

  @Test
  public void testCreateWithoutExpiry() {
    final Optional<Endpoint> maybeFrom = Optional.empty();
    final Endpoint to = Mockito.mock(Endpoint.class);
    final UInt64 enrSeq = UInt64.MAX_VALUE;

    PingPacketData result = factory.create(maybeFrom, to, enrSeq);

    Mockito.verify(endpointValidator).validate(to, "destination endpoint cannot be null");
    Mockito.verifyNoInteractions(expiryValidator);

    Assertions.assertEquals(maybeFrom, result.getFrom());
    Assertions.assertEquals(Optional.of(to), result.getTo());
    Assertions.assertEquals(clock.instant().getEpochSecond() + 60, result.getExpiration());
    Assertions.assertTrue(result.getEnrSeq().isPresent());
    Assertions.assertEquals(enrSeq, result.getEnrSeq().get());
  }

  @Test
  public void testCreateWithoutExpiryWithInvalidToAddress() {
    final Optional<Endpoint> maybeFrom = Optional.empty();
    final Endpoint to = Mockito.mock(Endpoint.class);
    final UInt64 enrSeq = UInt64.MAX_VALUE;

    Mockito.doThrow(new IllegalArgumentException())
        .when(endpointValidator)
        .validate(to, "destination endpoint cannot be null");

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> factory.create(maybeFrom, to, enrSeq));

    Mockito.verify(endpointValidator).validate(to, "destination endpoint cannot be null");
    Mockito.verifyNoInteractions(expiryValidator);
  }
}
