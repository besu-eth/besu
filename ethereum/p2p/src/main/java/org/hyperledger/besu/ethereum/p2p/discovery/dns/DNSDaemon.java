/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.p2p.discovery.dns;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import io.vertx.core.AbstractVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Adapted from https://github.com/tmio/tuweni and licensed under Apache 2.0
/**
 * Resolves DNS records over time, refreshing records. This is written as a Vertx Verticle which
 * allows to run outside the Vertx event loop
 */
public class DNSDaemon extends AbstractVerticle {
  private static final Logger LOG = LoggerFactory.getLogger(DNSDaemon.class);
  private final String enrLink;
  private final long seq;
  private final long initialDelay;
  private final long delay;
  private final Optional<DNSDaemonListener> listener;
  private final Optional<String> dnsServer;
  private Optional<Long> periodicTaskId = Optional.empty();
  private DNSResolver dnsResolver;

  /**
   * Creates a new DNSDaemon.
   *
   * @param enrLink the ENR link to start with, of the form enrtree://PUBKEY@domain
   * @param listener Listener notified when records are read and whenever they are updated.
   * @param seq the sequence number of the root record. If the root record seq is higher, proceed
   *     with visit.
   * @param initialDelay the delay in milliseconds before the first poll of DNS records.
   * @param delay the delay in milliseconds at which to poll DNS records. If negative or zero, it
   *     runs only once.
   * @param dnsServer the DNS server to use for DNS query. If null, the default DNS server will be
   *     used.
   */
  public DNSDaemon(
      final String enrLink,
      final DNSDaemonListener listener,
      final long seq,
      final long initialDelay,
      final long delay,
      final String dnsServer) {
    this.enrLink = enrLink;
    this.listener = Optional.ofNullable(listener);
    this.seq = seq;
    this.initialDelay = initialDelay;
    this.delay = delay;
    this.dnsServer = Optional.ofNullable(dnsServer);
  }

  /** Starts the DNSDaemon. */
  @Override
  public void start() {
    if (vertx == null) {
      throw new IllegalStateException("DNSDaemon must be deployed as a vertx verticle.");
    }

    LOG.info("Starting DNSDaemon for {}, using {} DNS host.", enrLink, dnsServer.orElse("default"));
    // a refresh stops reading before the next one is due, so that one slow refresh does not hold up
    // the next ones
    final Duration refreshTimeLimit =
        delay > 0 ? Duration.ofMillis(delay / 2) : DNSResolver.DEFAULT_REFRESH_TIME_LIMIT;
    dnsResolver =
        new DNSResolver(
            vertx,
            enrLink,
            seq,
            dnsServer,
            DNSResolver.MAX_RECORDS_PER_REFRESH,
            refreshTimeLimit,
            System::nanoTime);
    if (delay > 0) {
      periodicTaskId = Optional.of(vertx.setPeriodic(initialDelay, delay, this::refreshENRRecords));
    } else {
      // do one-shot resolution
      refreshENRRecords(0L);
    }
  }

  /** Stops the DNSDaemon. */
  @Override
  public void stop() {
    LOG.info("Stopping DNSDaemon for {}", enrLink);
    periodicTaskId.ifPresent(vertx::cancelTimer);
  }

  /**
   * Refresh enr records by calling dnsResolver and updating the listeners.
   *
   * @param taskId the task id of the periodic task
   */
  void refreshENRRecords(final Long taskId) {
    LOG.debug("Refreshing DNS records");
    final long startTime = System.nanoTime();
    final List<EthereumNodeRecord> ethereumNodeRecords = dnsResolver.collectAll();
    final long durationMs = (System.nanoTime() - startTime) / 1_000_000;
    LOG.debug(
        "Refreshed ENR DNS records: collected {} record(s) in {} ms",
        ethereumNodeRecords.size(),
        durationMs);
    listener.ifPresent(it -> it.newRecords(dnsResolver.sequence(), ethereumNodeRecords));
  }
}
