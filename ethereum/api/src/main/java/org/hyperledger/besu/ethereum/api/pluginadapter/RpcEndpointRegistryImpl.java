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
package org.hyperledger.besu.ethereum.api.pluginadapter;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.PluginJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.plugin.RegistrationClosedException;
import org.hyperledger.besu.plugin.services.RpcEndpointRegistry;
import org.hyperledger.besu.plugin.services.rpc.PluginRpcRequest;
import org.hyperledger.besu.util.KeyedRegistrations;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Collects the RPC endpoints plugins register, for the JSON-RPC method tables to pick up. */
public class RpcEndpointRegistryImpl implements RpcEndpointRegistry {

  private final KeyedRegistrations<Function<PluginRpcRequest, ?>> rpcMethods;
  private final BooleanSupplier registrationOpen;
  private final Predicate<String> pluginRunning;

  /** Creates a registry that accepts endpoints at any time. */
  public RpcEndpointRegistryImpl() {
    this(() -> true);
  }

  /**
   * Creates a registry whose registrations all count as Besu's own.
   *
   * @param registrationOpen whether plugin registration is currently open; endpoints registered
   *     while it is closed are rejected
   */
  public RpcEndpointRegistryImpl(final BooleanSupplier registrationOpen) {
    this(registrationOpen, Optional::empty);
  }

  /**
   * Creates a registry that serves every plugin's methods from the moment they are registered.
   *
   * @param registrationOpen whether plugin registration is currently open; endpoints registered
   *     while it is closed are rejected
   * @param currentRegistrant the id of the plugin whose register() is running, empty when Besu
   *     itself is registering; a method name already registered by another registrant is rejected
   */
  public RpcEndpointRegistryImpl(
      final BooleanSupplier registrationOpen, final Supplier<Optional<String>> currentRegistrant) {
    this(registrationOpen, currentRegistrant, pluginId -> true);
  }

  /**
   * Creates the registry.
   *
   * @param registrationOpen whether plugin registration is currently open; endpoints registered
   *     while it is closed are rejected
   * @param currentRegistrant the id of the plugin whose register() is running, empty when Besu
   *     itself is registering; a method name already registered by another registrant is rejected
   * @param pluginRunning whether the plugin with the given id has returned from start() and not yet
   *     from stop(); a call to a plugin's method is answered with {@link
   *     RpcErrorType#PLUGIN_NOT_RUNNING} while it is not
   */
  public RpcEndpointRegistryImpl(
      final BooleanSupplier registrationOpen,
      final Supplier<Optional<String>> currentRegistrant,
      final Predicate<String> pluginRunning) {
    this.rpcMethods = new KeyedRegistrations<>("RPC method", currentRegistrant);
    this.registrationOpen = registrationOpen;
    this.pluginRunning = pluginRunning;
  }

  @Override
  public <T> void registerRPCEndpoint(
      final String namespace,
      final String functionName,
      final Function<PluginRpcRequest, T> function) {
    if (!registrationOpen.getAsBoolean()) {
      throw new RegistrationClosedException("RpcEndpointRegistry", "registerRPCEndpoint");
    }
    checkArgument(namespace.matches("\\p{Alnum}+"), "Namespace must be only alpha numeric");
    checkArgument(functionName.matches("\\p{Alnum}+"), "Function Name must be only alpha numeric");
    checkNotNull(function);

    rpcMethods.register(namespace + "_" + functionName, function);
  }

  /**
   * Gets plugin methods.
   *
   * @param namespaces the namespaces collection
   * @return the Json Rpc Methods from plugins
   */
  public Map<String, ? extends JsonRpcMethod> getPluginMethods(
      final Collection<String> namespaces) {
    return rpcMethods.byName().entrySet().stream()
        .filter(
            entry ->
                namespaces.stream()
                    .anyMatch(
                        namespace ->
                            entry
                                .getKey()
                                .toUpperCase(Locale.ROOT)
                                .startsWith(namespace.toUpperCase(Locale.ROOT))))
        .map(
            entry ->
                gated(entry.getKey(), new PluginJsonRpcMethod(entry.getKey(), entry.getValue())))
        .collect(Collectors.toMap(JsonRpcMethod::getName, e -> e));
  }

  /**
   * Checks if RPC methods belongs to a namespace
   *
   * @param namespace the namespace to check against
   * @return true if any of the RPC method starts with given namespace, false otherwise.
   */
  public boolean hasNamespace(final String namespace) {
    return rpcMethods.names().stream()
        .anyMatch(
            key -> key.toUpperCase(Locale.ROOT).startsWith(namespace.toUpperCase(Locale.ROOT)));
  }

  /** Wraps a plugin's method so it is refused while that plugin is not running. */
  private JsonRpcMethod gated(final String name, final JsonRpcMethod method) {
    return rpcMethods
        .registrantOf(name)
        .filter(registrant -> !KeyedRegistrations.BESU.equals(registrant))
        .<JsonRpcMethod>map(pluginId -> new NotRunningGuard(pluginId, method, pluginRunning))
        .orElse(method);
  }

  private static final class NotRunningGuard implements JsonRpcMethod {
    private final String pluginId;
    private final JsonRpcMethod delegate;
    private final Predicate<String> pluginRunning;

    NotRunningGuard(
        final String pluginId,
        final JsonRpcMethod delegate,
        final Predicate<String> pluginRunning) {
      this.pluginId = pluginId;
      this.delegate = delegate;
      this.pluginRunning = pluginRunning;
    }

    @Override
    public String getName() {
      return delegate.getName();
    }

    @Override
    public JsonRpcResponse response(final JsonRpcRequestContext request) {
      if (!pluginRunning.test(pluginId)) {
        return new JsonRpcErrorResponse(
            request.getRequest().getId(), RpcErrorType.PLUGIN_NOT_RUNNING, pluginId);
      }
      return delegate.response(request);
    }
  }
}
