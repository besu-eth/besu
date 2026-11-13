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

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.PluginJsonRpcMethod;
import org.hyperledger.besu.plugin.RegistrationClosedException;
import org.hyperledger.besu.plugin.services.RpcEndpointRegistry;
import org.hyperledger.besu.plugin.services.rpc.PluginRpcRequest;

import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Collects the RPC endpoints plugins register, for the JSON-RPC method tables to pick up. */
public class RpcEndpointRegistryImpl implements RpcEndpointRegistry {

  /** The registrant of an entry Besu registered itself, outside any plugin's register(). */
  private static final String BESU = "Besu";

  /** A registered method with the id of the plugin that registered it. */
  private record Registered(String registrant, Function<PluginRpcRequest, ?> function) {}

  private final Map<String, Registered> rpcMethods = new HashMap<>();
  private final BooleanSupplier registrationOpen;
  private final Supplier<Optional<String>> currentRegistrant;

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
   * Creates the registry.
   *
   * @param registrationOpen whether plugin registration is currently open; endpoints registered
   *     while it is closed are rejected
   * @param currentRegistrant the id of the plugin whose register() is running, empty when Besu
   *     itself is registering; a method name already registered by another registrant is rejected
   */
  public RpcEndpointRegistryImpl(
      final BooleanSupplier registrationOpen, final Supplier<Optional<String>> currentRegistrant) {
    this.registrationOpen = registrationOpen;
    this.currentRegistrant = currentRegistrant;
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

    final String name = namespace + "_" + functionName;
    final String registrant = currentRegistrant.get().orElse(BESU);
    final Registered previous = rpcMethods.get(name);
    if (previous != null && !previous.registrant().equals(registrant)) {
      throw new IllegalStateException(
          "cannot register RPC method "
              + name
              + ": "
              + owner(previous.registrant())
              + " already registered it");
    }
    rpcMethods.put(name, new Registered(registrant, function));
  }

  private static String owner(final String registrant) {
    return BESU.equals(registrant) ? BESU : "plugin " + registrant;
  }

  /**
   * Gets plugin methods.
   *
   * @param namespaces the namespaces collection
   * @return the Json Rpc Methods from plugins
   */
  public Map<String, ? extends JsonRpcMethod> getPluginMethods(
      final Collection<String> namespaces) {
    return rpcMethods.entrySet().stream()
        .filter(
            entry ->
                namespaces.stream()
                    .anyMatch(
                        namespace ->
                            entry
                                .getKey()
                                .toUpperCase(Locale.ROOT)
                                .startsWith(namespace.toUpperCase(Locale.ROOT))))
        .map(entry -> new PluginJsonRpcMethod(entry.getKey(), entry.getValue().function()))
        .collect(Collectors.toMap(PluginJsonRpcMethod::getName, e -> e));
  }

  /**
   * Checks if RPC methods belongs to a namespace
   *
   * @param namespace the namespace to check against
   * @return true if any of the RPC method starts with given namespace, false otherwise.
   */
  public boolean hasNamespace(final String namespace) {
    return rpcMethods.keySet().stream()
        .anyMatch(
            key -> key.toUpperCase(Locale.ROOT).startsWith(namespace.toUpperCase(Locale.ROOT)));
  }
}
