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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.Nullable;
import org.jupnp.DefaultUpnpServiceConfiguration;
import org.jupnp.UpnpServiceConfiguration;
import org.jupnp.binding.xml.DeviceDescriptorBinder;
import org.jupnp.binding.xml.ServiceDescriptorBinder;
import org.jupnp.binding.xml.UDA10DeviceDescriptorBinderImpl;
import org.jupnp.binding.xml.UDA10ServiceDescriptorBinderImpl;
import org.jupnp.model.Namespace;
import org.jupnp.model.message.UpnpHeaders;
import org.jupnp.model.meta.RemoteDeviceIdentity;
import org.jupnp.model.meta.RemoteService;
import org.jupnp.model.types.ServiceType;
import org.jupnp.transport.impl.DatagramIOConfigurationImpl;
import org.jupnp.transport.impl.DatagramIOImpl;
import org.jupnp.transport.impl.DatagramProcessorImpl;
import org.jupnp.transport.impl.GENAEventProcessorImpl;
import org.jupnp.transport.impl.MulticastReceiverConfigurationImpl;
import org.jupnp.transport.impl.MulticastReceiverImpl;
import org.jupnp.transport.impl.NetworkAddressFactoryImpl;
import org.jupnp.transport.impl.SOAPActionProcessorImpl;
import org.jupnp.transport.impl.jetty.StreamClientConfigurationImpl;
import org.jupnp.transport.spi.DatagramIO;
import org.jupnp.transport.spi.DatagramProcessor;
import org.jupnp.transport.spi.GENAEventProcessor;
import org.jupnp.transport.spi.MulticastReceiver;
import org.jupnp.transport.spi.NetworkAddressFactory;
import org.jupnp.transport.spi.SOAPActionProcessor;
import org.jupnp.transport.spi.StreamClient;
import org.jupnp.transport.spi.StreamServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BesuUpnpServiceConfiguration implements UpnpServiceConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(BesuUpnpServiceConfiguration.class);

  private final ThreadPoolExecutor executorService;
  private final DeviceDescriptorBinder deviceDescriptorBinderUDA10;
  private final ServiceDescriptorBinder serviceDescriptorBinderUDA10;
  private final int streamListenPort;
  private final int multicastResponsePort;
  private final Namespace namespace;

  BesuUpnpServiceConfiguration() {
    this(
        NetworkAddressFactoryImpl.DEFAULT_TCP_HTTP_LISTEN_PORT,
        NetworkAddressFactoryImpl.DEFAULT_MULTICAST_RESPONSE_LISTEN_PORT);
  }

  private BesuUpnpServiceConfiguration(
      final int streamListenPort, final int multicastResponsePort) {
    executorService =
        new ThreadPoolExecutor(
            16,
            200,
            10,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(2000),
            new DefaultUpnpServiceConfiguration.JUPnPThreadFactory(),
            new ThreadPoolExecutor.DiscardPolicy() {
              // The pool is bounded and rejections will happen during shutdown
              @Override
              public void rejectedExecution(
                  final Runnable runnable, final ThreadPoolExecutor threadPoolExecutor) {
                // Log and discard
                LOG.warn("Thread pool rejected execution of " + runnable.getClass());
                super.rejectedExecution(runnable, threadPoolExecutor);
              }
            });
    executorService.allowCoreThreadTimeOut(true);

    deviceDescriptorBinderUDA10 = new UDA10DeviceDescriptorBinderImpl();
    serviceDescriptorBinderUDA10 = new UDA10ServiceDescriptorBinderImpl();
    namespace = new Namespace();

    this.streamListenPort = streamListenPort;
    this.multicastResponsePort = multicastResponsePort;
  }

  @Override
  public NetworkAddressFactory createNetworkAddressFactory() {
    return new NetworkAddressFactoryImpl(streamListenPort, multicastResponsePort);
  }

  @Override
  public DatagramProcessor getDatagramProcessor() {
    return new DatagramProcessorImpl();
  }

  @Override
  public SOAPActionProcessor getSoapActionProcessor() {
    return new SOAPActionProcessorImpl();
  }

  @Override
  public GENAEventProcessor getGenaEventProcessor() {
    return new GENAEventProcessorImpl();
  }

  @SuppressWarnings("rawtypes") // superclass uses raw types
  @Override
  public StreamClient createStreamClient() {
    return new OkHttpStreamClient(new StreamClientConfigurationImpl(executorService));
  }

  @SuppressWarnings("rawtypes") // superclass uses raw types
  @Override
  public MulticastReceiver createMulticastReceiver(
      final NetworkAddressFactory networkAddressFactory) {
    return new MulticastReceiverImpl(
        new MulticastReceiverConfigurationImpl(
            networkAddressFactory.getMulticastGroup(), networkAddressFactory.getMulticastPort()));
  }

  @SuppressWarnings("rawtypes") // superclass uses raw types
  @Override
  public DatagramIO createDatagramIO(final NetworkAddressFactory networkAddressFactory) {
    return new DatagramIOImpl(new DatagramIOConfigurationImpl());
  }

  @Override
  @SuppressWarnings("NullAway")
  public @Nullable StreamServer<?> createStreamServer(
      final NetworkAddressFactory networkAddressFactory) {
    return null;
  }

  @Override
  public Executor getMulticastReceiverExecutor() {
    return executorService;
  }

  @Override
  public Executor getDatagramIOExecutor() {
    return executorService;
  }

  @Override
  public ExecutorService getStreamServerExecutorService() {
    return executorService;
  }

  @Override
  public DeviceDescriptorBinder getDeviceDescriptorBinderUDA10() {
    return deviceDescriptorBinderUDA10;
  }

  @Override
  public ServiceDescriptorBinder getServiceDescriptorBinderUDA10() {
    return serviceDescriptorBinderUDA10;
  }

  @Override
  public ServiceType[] getExclusiveServiceTypes() {
    return new ServiceType[0];
  }

  @Override
  public int getRegistryMaintenanceIntervalMillis() {
    return 1000;
  }

  @Override
  public int getAliveIntervalMillis() {
    return 0;
  }

  @Override
  public boolean isReceivedSubscriptionTimeoutIgnored() {
    return false;
  }

  @Override
  @SuppressWarnings("NullAway")
  public @Nullable Integer getRemoteDeviceMaxAgeSeconds() {
    return null;
  }

  @Override
  @SuppressWarnings("NullAway")
  public @Nullable UpnpHeaders getDescriptorRetrievalHeaders(final RemoteDeviceIdentity identity) {
    return null;
  }

  @Override
  @SuppressWarnings("NullAway")
  public @Nullable UpnpHeaders getEventSubscriptionHeaders(final RemoteService service) {
    return null;
  }

  @Override
  public Executor getAsyncProtocolExecutor() {
    return executorService;
  }

  @Override
  public ExecutorService getSyncProtocolExecutorService() {
    return executorService;
  }

  @Override
  public Namespace getNamespace() {
    return namespace;
  }

  @Override
  public Executor getRegistryMaintainerExecutor() {
    return executorService;
  }

  @Override
  public Executor getRegistryListenerExecutor() {
    return executorService;
  }

  @Override
  public Executor getRemoteListenerExecutor() {
    return executorService;
  }

  @Override
  public void shutdown() {
    executorService.shutdown();
  }
}
