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
package org.hyperledger.besu.ethereum.p2p.peers;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Test-only resolver, registered for this module's tests through {@code META-INF/services}, that
 * gives one TEST-NET-3 address a reverse (PTR) record, so tests can tell whether code performs a
 * reverse lookup without depending on real DNS. Every other query goes to the built-in resolver.
 */
public class ReverseDnsTestResolverProvider extends InetAddressResolverProvider {

  /** Address with a PTR record in this resolver only (RFC 5737 documentation range). */
  static final String ADDRESS_WITH_PTR = "203.0.113.9";

  /** Name that {@link #ADDRESS_WITH_PTR} reverse-resolves to, and that forward-resolves to it. */
  static final String PTR_NAME = "ptr.reverse-zone.example";

  /** Number of reverse lookups of {@link #ADDRESS_WITH_PTR} performed so far. */
  static final AtomicInteger REVERSE_LOOKUPS = new AtomicInteger();

  private static final byte[] ADDRESS_WITH_PTR_BYTES = {(byte) 203, 0, 113, 9};

  @Override
  public InetAddressResolver get(final Configuration configuration) {
    final InetAddressResolver builtin = configuration.builtinResolver();
    return new InetAddressResolver() {
      @Override
      public Stream<InetAddress> lookupByName(final String host, final LookupPolicy lookupPolicy)
          throws UnknownHostException {
        if (PTR_NAME.equalsIgnoreCase(host)) {
          return Stream.of(InetAddress.getByAddress(host, ADDRESS_WITH_PTR_BYTES));
        }
        return builtin.lookupByName(host, lookupPolicy);
      }

      @Override
      public String lookupByAddress(final byte[] addr) throws UnknownHostException {
        if (Arrays.equals(addr, ADDRESS_WITH_PTR_BYTES)) {
          REVERSE_LOOKUPS.incrementAndGet();
          return PTR_NAME;
        }
        return builtin.lookupByAddress(addr);
      }
    };
  }

  @Override
  public String name() {
    return "besu-p2p-test-reverse-dns";
  }
}
