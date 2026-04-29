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
package org.hyperledger.besu.nat.upnp;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import org.hyperledger.besu.nat.NatMethod;
import org.hyperledger.besu.nat.core.AbstractNatManager;
import org.hyperledger.besu.nat.core.domain.NatPortMapping;
import org.hyperledger.besu.nat.core.domain.NatServiceType;
import org.hyperledger.besu.nat.core.domain.NetworkProtocol;
import org.hyperledger.besu.nat.core.exception.NatInitializationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.jupnp.UpnpService;
import org.jupnp.UpnpServiceImpl;
import org.jupnp.model.action.ActionInvocation;
import org.jupnp.model.message.UpnpResponse;
import org.jupnp.model.meta.RemoteDevice;
import org.jupnp.model.meta.RemoteDeviceIdentity;
import org.jupnp.model.meta.RemoteService;
import org.jupnp.model.types.UnsignedIntegerFourBytes;
import org.jupnp.model.types.UnsignedIntegerTwoBytes;
import org.jupnp.registry.Registry;
import org.jupnp.registry.RegistryListener;
import org.jupnp.support.igd.callback.GetExternalIP;
import org.jupnp.support.igd.callback.PortMappingAdd;
import org.jupnp.support.igd.callback.PortMappingDelete;
import org.jupnp.support.model.PortMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages underlying UPnP library "jupnp" and provides abstractions for asynchronously interacting
 * with the NAT environment through UPnP.
 */
public class UpnpNatManager extends AbstractNatManager {
  private static final Logger LOG = LoggerFactory.getLogger(UpnpNatManager.class);

  static final String SERVICE_TYPE_WAN_IP_CONNECTION = "WANIPConnection";

  private final UpnpService upnpService;
  private final RegistryListener registryListener;

  // internally-managed future. external queries for IP addresses will be copy()ed from this.
  private final CompletableFuture<String> externalIpQueryFuture = new CompletableFuture<>();

  private final Map<String, CompletableFuture<RemoteService>> recognizedServices = new HashMap<>();
  private final List<NatPortMapping> forwardedPorts = new ArrayList<>();
  private Optional<String> discoveredOnLocalAddress = Optional.empty();

  /** Empty constructor. Creates in instance of UpnpServiceImpl. */
  public UpnpNatManager() {
    // this(new UpnpServiceImpl(new DefaultUpnpServiceConfiguration()));

    // Workaround for an issue in the jupnp library: the ExecutorService used misconfigures
    // its ThreadPoolExecutor, causing it to only launch a single thread. This prevents any work
    // from getting done (effectively a deadlock). The issue is fixed here:
    //   https://github.com/jupnp/jupnp/pull/117
    // However, this fix has not made it into any releases yet.
    // TODO: once a new release is available, remove this @Override
    this(new UpnpServiceImpl(new BesuUpnpServiceConfiguration()));
  }

  /**
   * Constructor
   *
   * @param service is the desired instance of UpnpService.
   */
  UpnpNatManager(final UpnpService service) {
    super(NatMethod.UPNP);
    upnpService = service;

    // prime our recognizedServices map so we can use its key-set later
    recognizedServices.put(SERVICE_TYPE_WAN_IP_CONNECTION, new CompletableFuture<>());

    // registry listener to observe new devices and look for specific services
    registryListener =
        new BesuUpnpRegistryListener() {
          @Override
          public void remoteDeviceAdded(final Registry registry, final RemoteDevice device) {
            LOG.atDebug()
                .setMessage("UPnP Device discovered: {}")
                .addArgument(device.getDetails().getFriendlyName())
                .log();
            inspectDeviceRecursive(device, recognizedServices.keySet());
          }
        };
  }

  /**
   * Start the manager. Must not be in started state.
   *
   * @throws IllegalStateException if already started.
   */
  @Override
  public synchronized void doStart() throws NatInitializationException {
    LOG.info("Starting UPnP Service");
    try {
      upnpService.startup();
      upnpService.getRegistry().addListener(registryListener);
      initiateExternalIpQuery();
    } catch (Exception e) {
      throw new NatInitializationException("Failed start UPnP nat service.", e);
    }
  }

  /**
   * Stop the manager. Must not be in stopped state.
   *
   * @throws IllegalStateException if stopped.
   */
  @Override
  public synchronized void doStop() {
    CompletableFuture<Void> portForwardReleaseFuture = releaseAllPortForwards();
    try {
      LOG.info("Allowing 3 seconds to release all port forwards...");
      portForwardReleaseFuture.get(3, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.warn("Caught exception while trying to release port forwards, ignoring", e);
    }

    for (CompletableFuture<RemoteService> future : recognizedServices.values()) {
      future.cancel(true);
    }

    upnpService.getRegistry().removeListener(registryListener);
    upnpService.shutdown();
  }

  /**
   * Returns the first of the discovered services of the given type, if any.
   *
   * @param type is the type descriptor of the desired service
   * @return a CompletableFuture that will provide the desired RemoteService when completed
   */
  private synchronized CompletableFuture<RemoteService> getService(final String type) {
    return recognizedServices.computeIfAbsent(type, unused -> new CompletableFuture<>());
  }

  /**
   * Get the discovered WANIPConnection service, if any.
   *
   * @return a CompletableFuture that will provide the WANIPConnection RemoteService when completed
   */
  @VisibleForTesting
  synchronized CompletableFuture<RemoteService> getWANIPConnectionService() {
    checkState(isStarted(), "Cannot call getWANIPConnectionService() when in stopped state");
    return getService(SERVICE_TYPE_WAN_IP_CONNECTION);
  }

  /**
   * Returns a CompletableFuture that will wait for the given service type to be discovered. No new
   * query will be performed, and if the service has already been discovered, the future will
   * complete in the very near future.
   *
   * @param serviceType is the service type to wait to be discovered.
   * @return future that will return the desired service once it is discovered, or null if the
   *     future is cancelled.
   */
  private synchronized CompletableFuture<RemoteService> discoverService(final String serviceType) {
    return getService(serviceType);
  }

  /**
   * Sends a UPnP request to the discovered IGD for the external ip address.
   *
   * @return A CompletableFuture that can be used to query the result (or error).
   */
  @Override
  public CompletableFuture<String> retrieveExternalIPAddress() {
    return externalIpQueryFuture.thenApply(x -> x);
  }

  /**
   * Returns all known port mappings.
   *
   * @return The known port mappings wrapped in a {@link java.util.concurrent.Future}.
   */
  @Override
  public CompletableFuture<List<NatPortMapping>> getPortMappings() {
    checkState(isStarted(), "Cannot call getPortMappings() when in stopped state");
    return CompletableFuture.completedFuture(forwardedPorts);
  }

  /**
   * Sends a UPnP request to the discovered IGD for the external ip address.
   *
   * <p>Note that this is not synchronized, as it is expected to be called within an
   * already-synchronized context ({@link #start()}).
   */
  private void initiateExternalIpQuery() {
    discoverService(SERVICE_TYPE_WAN_IP_CONNECTION)
        .thenAccept(
            service -> {

              // our query, which will be handled asynchronously by the jupnp library
              GetExternalIP callback =
                  new GetExternalIP(service) {

                    /**
                     * Override the success(ActionInvocation) version of success so that we can take
                     * a peek at the network interface that we discovered this on.
                     *
                     * <p>Because the underlying jupnp library omits generics info in this method
                     * signature, we must too when we override it.
                     */
                    @Override
                    @SuppressWarnings("rawtypes")
                    public void success(final ActionInvocation invocation) {
                      RemoteService service = (RemoteService) invocation.getAction().getService();
                      RemoteDevice device = service.getDevice();
                      RemoteDeviceIdentity identity = device.getIdentity();

                      discoveredOnLocalAddress =
                          Optional.of(identity.getDiscoveredOnLocalAddress().getHostAddress());

                      super.success(invocation);
                    }

                    @Override
                    protected void success(final String result) {

                      LOG.info(
                          "External IP address {} detected for internal address {}",
                          result,
                          discoveredOnLocalAddress.orElse("unknown"));

                      externalIpQueryFuture.complete(result);
                    }

                    /**
                     * Because the underlying jupnp library omits generics info in this method
                     * signature, we must too when we override it.
                     */
                    @Override
                    @SuppressWarnings("rawtypes")
                    public void failure(
                        final ActionInvocation invocation,
                        final UpnpResponse operation,
                        final String msg) {
                      externalIpQueryFuture.completeExceptionally(new Exception(msg));
                    }
                  };
              upnpService.getControlPoint().execute(callback);
            });
  }

  /**
   * Convenience function to call {@link #requestPortForward(PortMapping)} with the following
   * defaults:
   *
   * <p>enabled: true leaseDurationSeconds: 0 (indefinite) remoteHost: null internalClient: the
   * local address used to discover gateway
   *
   * <p>In addition, port is used for both internal and external port values.
   *
   * @param port is the port to be used for both internal and external port values
   * @param protocol is either UDP or TCP {@link NetworkProtocol}
   * @param serviceType {@link NatServiceType}, often displayed in router UIs
   */
  public void requestPortForward(
      final int port, final NetworkProtocol protocol, final NatServiceType serviceType) {
    checkState(isStarted(), "Cannot call requestPortForward() when in stopped state");
    checkArgument(port != 0, "Cannot map to internal port zero.");
    this.requestPortForward(
        new PortMapping(
            true,
            new UnsignedIntegerFourBytes(0),
            null,
            new UnsignedIntegerTwoBytes(port),
            new UnsignedIntegerTwoBytes(port),
            null,
            toJupnpProtocol(protocol),
            serviceType.getValue()));
  }

  /**
   * Sends a UPnP request to the discovered IGD to request a port forward.
   *
   * @param portMapping is a portMapping object describing the desired port mapping parameters.
   * @return A CompletableFuture that can be used to query the result (or error).
   */
  private CompletableFuture<Void> requestPortForward(final PortMapping portMapping) {

    CompletableFuture<Void> upnpQueryFuture = new CompletableFuture<>();

    return externalIpQueryFuture.thenCompose(
        address -> {
          // note that this future is a dependency of externalIpQueryFuture, so it must be completed
          // by now
          RemoteService service = getService(SERVICE_TYPE_WAN_IP_CONNECTION).join();

          // at this point, we should have the local address we discovered the IGD on,
          // so we can prime the NewInternalClient field if it was omitted
          if (null == portMapping.getInternalClient()) {
            portMapping.setInternalClient(
                discoveredOnLocalAddress.orElseThrow(
                    () -> new IllegalStateException("No local address discovered for UPnP")));
          }

          // our query, which will be handled asynchronously by the jupnp library
          PortMappingAdd callback =
              new PortMappingAdd(service, portMapping) {
                /**
                 * Because the underlying jupnp library omits generics info in this method
                 * signature, we must too when we override it.
                 */
                @Override
                @SuppressWarnings("rawtypes")
                public void success(final ActionInvocation invocation) {
                  LOG.info(
                      "Port forward request for {} {} -> {} succeeded.",
                      portMapping.getProtocol(),
                      portMapping.getInternalPort(),
                      portMapping.getExternalPort());

                  synchronized (forwardedPorts) {
                    final NatServiceType natServiceType =
                        NatServiceType.fromString(requireNonNull(portMapping.getDescription()));
                    final NatPortMapping natPortMapping =
                        new NatPortMapping(
                            natServiceType,
                            NetworkProtocol.valueOf(portMapping.getProtocol().name()),
                            requireNonNull(portMapping.getInternalClient()),
                            portMapping.getRemoteHost(),
                            portMapping.getExternalPort().getValue().intValue(),
                            portMapping.getInternalPort().getValue().intValue());
                    forwardedPorts.add(natPortMapping);
                  }

                  upnpQueryFuture.complete(null);
                }

                /**
                 * Because the underlying jupnp library omits generics info in this method
                 * signature, we must too when we override it.
                 */
                @Override
                @SuppressWarnings("rawtypes")
                public void failure(
                    final ActionInvocation invocation,
                    final UpnpResponse operation,
                    final String msg) {
                  LOG.warn(
                      "Port forward request for {} {} -> {} failed: {}",
                      portMapping.getProtocol(),
                      portMapping.getInternalPort(),
                      portMapping.getExternalPort(),
                      msg);
                  upnpQueryFuture.completeExceptionally(new Exception(msg));
                }
              };

          LOG.info(
              "Requesting port forward for {} {} -> {}",
              portMapping.getProtocol(),
              portMapping.getInternalPort(),
              portMapping.getExternalPort());

          upnpService.getControlPoint().execute(callback);

          return upnpQueryFuture;
        });
  }

  /**
   * Attempts to release any forwarded ports.
   *
   * <p>Note that this is not synchronized, as it is expected to be called within an
   * already-synchronized context ({@link #stop()}).
   *
   * @return A CompletableFuture that will complete when all port forward requests have been made
   */
  private CompletableFuture<Void> releaseAllPortForwards() {
    // if we haven't observed the WANIPConnection service yet, we should have no port forwards to
    // release
    CompletableFuture<RemoteService> wanIPConnectionServiceFuture =
        getService(SERVICE_TYPE_WAN_IP_CONNECTION);
    if (!wanIPConnectionServiceFuture.isDone()) {
      return CompletableFuture.completedFuture(null);
    }

    RemoteService service = wanIPConnectionServiceFuture.join();

    List<CompletableFuture<Void>> futures = new ArrayList<>();

    boolean done = false;
    while (!done) {
      NatPortMapping portMapping;
      synchronized (forwardedPorts) {
        if (forwardedPorts.isEmpty()) {
          done = true;
          continue;
        }

        portMapping = forwardedPorts.get(0);
        forwardedPorts.remove(0);
      }

      LOG.info(
          "Releasing port forward for {} {} -> {}",
          portMapping.getProtocol(),
          portMapping.getInternalPort(),
          portMapping.getExternalPort());

      CompletableFuture<Void> future = new CompletableFuture<>();

      PortMappingDelete callback =
          new PortMappingDelete(service, toJupnpPortMapping(portMapping)) {
            /**
             * Because the underlying jupnp library omits generics info in this method signature, we
             * must too when we override it.
             */
            @Override
            @SuppressWarnings("rawtypes")
            public void success(final ActionInvocation invocation) {
              LOG.info(
                  "Port forward {} {} -> {} removed successfully.",
                  portMapping.getProtocol(),
                  portMapping.getInternalPort(),
                  portMapping.getExternalPort());

              future.complete(null);
            }

            /**
             * Because the underlying jupnp library omits generics info in this method signature, we
             * must too when we override it.
             */
            @Override
            @SuppressWarnings("rawtypes")
            public void failure(
                final ActionInvocation invocation, final UpnpResponse operation, final String msg) {
              LOG.warn(
                  "Port forward removal request for {} {} -> {} failed (ignoring): {}",
                  portMapping.getProtocol(),
                  portMapping.getInternalPort(),
                  portMapping.getExternalPort(),
                  msg);

              // ignore exceptions; we did our best
              future.complete(null);
            }
          };

      upnpService.getControlPoint().execute(callback);

      futures.add(future);
    }

    // return a future that completes successfully only when each of our port delete requests
    // complete
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
  }

  /**
   * Recursively crawls the given device to look for specific services.
   *
   * @param device is the device to inspect for desired services.
   * @param serviceTypes is a set of service types to look for.
   */
  private void inspectDeviceRecursive(final RemoteDevice device, final Set<String> serviceTypes) {
    for (RemoteService service : device.getServices()) {
      String serviceType = service.getServiceType().getType();
      if (serviceTypes.contains(serviceType)) {
        synchronized (this) {
          // log a warning if we detect a second WANIPConnection service
          CompletableFuture<RemoteService> existingFuture = recognizedServices.get(serviceType);
          if (existingFuture == null) {
            existingFuture = getService(serviceType);
          }
          if (existingFuture.isDone()) {
            LOG.warn(
                "Detected multiple WANIPConnection services on network. This may interfere with NAT circumvention.");
            continue;
          }
          existingFuture.complete(service);
        }
      }
    }
    for (RemoteDevice subDevice : device.getEmbeddedDevices()) {
      inspectDeviceRecursive(subDevice, serviceTypes);
    }
  }

  private PortMapping.Protocol toJupnpProtocol(final NetworkProtocol protocol) {
    switch (protocol) {
      case UDP:
        return PortMapping.Protocol.UDP;
      case TCP:
        return PortMapping.Protocol.TCP;
    }
    throw new IllegalStateException("Unsupported network protocol: " + protocol);
  }

  private PortMapping toJupnpPortMapping(final NatPortMapping natPortMapping) {
    return new PortMapping(
        true,
        new UnsignedIntegerFourBytes(0),
        null,
        new UnsignedIntegerTwoBytes(natPortMapping.getExternalPort()),
        new UnsignedIntegerTwoBytes(natPortMapping.getInternalPort()),
        null,
        toJupnpProtocol(natPortMapping.getProtocol()),
        natPortMapping.getNatServiceType().getValue());
  }
}
