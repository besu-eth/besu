/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.nat.core;

import static com.google.common.base.Preconditions.checkState;

import org.hyperledger.besu.nat.NatMethod;
import org.hyperledger.besu.nat.core.domain.NatPortMapping;
import org.hyperledger.besu.nat.core.domain.NatServiceType;
import org.hyperledger.besu.nat.core.domain.NetworkProtocol;
import org.hyperledger.besu.nat.core.exception.NatInitializationException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Abstract Nat manager. */
public abstract class AbstractNatManager implements NatManager {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractNatManager.class);

  /** The Nat method. */
  protected final NatMethod natMethod;

  /** The Started. */
  protected final AtomicBoolean started = new AtomicBoolean();

  /**
   * Instantiates a new Abstract nat manager.
   *
   * @param natMethod the nat method
   */
  protected AbstractNatManager(final NatMethod natMethod) {
    this.natMethod = natMethod;
  }

  /**
   * Do start.
   *
   * @throws NatInitializationException the nat initialization exception
   */
  protected abstract void doStart() throws NatInitializationException;

  /** Do stop. */
  protected abstract void doStop();

  /**
   * Retrieve external ip address completable future.
   *
   * @return the completable future
   */
  protected abstract CompletableFuture<String> retrieveExternalIPAddress();

  @Override
  public NatMethod getNatMethod() {
    return natMethod;
  }

  @Override
  public boolean isStarted() {
    return started.get();
  }

  @Override
  public CompletableFuture<String> queryExternalIPAddress() {
    checkState(isStarted(), "Cannot call queryExternalIPAddress() when in stopped state");
    return retrieveExternalIPAddress();
  }

  @Override
  public CompletableFuture<String> queryLocalIPAddress() {
    final CompletableFuture<String> future = new CompletableFuture<>();
    Executors.newCachedThreadPool()
        .submit(
            () -> {
              try {
                future.complete(InetAddress.getLocalHost().getHostAddress());
              } catch (UnknownHostException e) {
                future.completeExceptionally(e);
              }
            });
    return future;
  }

  @Override
  public void start() throws NatInitializationException {
    if (started.compareAndSet(false, true)) {
      doStart();
    } else {
      LOG.warn("Attempt to start an already-started {}", getClass().getSimpleName());
    }
  }

  @Override
  public void stop() {
    if (started.compareAndSet(true, false)) {
      doStop();
    } else {
      LOG.warn("Attempt to stop an already-stopped {}", getClass().getSimpleName());
    }
  }

  @Override
  public NatPortMapping getPortMapping(
      final NatServiceType serviceType, final NetworkProtocol networkProtocol) {
    try {
      final List<NatPortMapping> natPortMappings =
          getPortMappings().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      final Optional<NatPortMapping> foundPortMapping =
          natPortMappings.stream()
              .filter(
                  c ->
                      c.getNatServiceType().equals(serviceType)
                          && c.getProtocol().equals(networkProtocol))
              .findFirst();
      return foundPortMapping.orElseThrow(
          () ->
              new IllegalArgumentException(
                  String.format(
                      "Required service type not found : %s %s", serviceType, networkProtocol)));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          String.format("Unable to retrieve the service type : %s", serviceType.toString()));
    } catch (ExecutionException | TimeoutException e) {
      throw new RuntimeException(
          String.format("Unable to retrieve the service type : %s", serviceType.toString()));
    }
  }
}
