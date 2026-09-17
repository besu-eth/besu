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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.plugin.BesuPlugin;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PluginsReloadConfiguration implements JsonRpcMethod {

  private static final Logger LOG = LoggerFactory.getLogger(PluginsReloadConfiguration.class);
  private final Map<String, BesuPlugin> namedPlugins;

  public PluginsReloadConfiguration(final Map<String, BesuPlugin> namedPlugins) {
    this.namedPlugins = namedPlugins;
  }

  @Override
  public String getName() {
    return RpcMethod.PLUGINS_RELOAD_CONFIG.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    try {
      final var maybePluginName = requestContext.getOptionalParameter(0, String.class);

      final Map<String, BesuPlugin> reloadPluginsByName;
      if (maybePluginName.isPresent()) {
        final var pluginName = maybePluginName.get();
        if (!namedPlugins.containsKey(pluginName)) {
          LOG.error(
              "Plugin cannot be reloaded because no plugin has been registered with specified name: {}",
              pluginName);
          return new JsonRpcErrorResponse(
              requestContext.getRequest().getId(), RpcErrorType.PLUGIN_NOT_FOUND);
        }
        reloadPluginsByName = Collections.singletonMap(pluginName, namedPlugins.get(pluginName));
      } else {
        reloadPluginsByName = namedPlugins;
      }

      final var asyncReloads =
          reloadPluginsByName.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey, e -> asyncReloadPluginConfig(e.getKey(), e.getValue())));

      try {
        CompletableFuture.allOf(
                asyncReloads.values().toArray(new CompletableFuture<?>[asyncReloads.size()]))
            .get();
      } catch (InterruptedException | ExecutionException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        // fill a detailed error message with the result of each plugin
        final String errMsg =
            asyncReloads.entrySet().stream()
                .map(this::resultToString)
                .collect(Collectors.joining(","));
        return new JsonRpcErrorResponse(
            requestContext.getRequest().getId(), new JsonRpcError(-32000, errMsg, null));
      }

      return new JsonRpcSuccessResponse(requestContext.getRequest().getId());
    } catch (JsonRpcParameterException e) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(), RpcErrorType.INVALID_PLUGIN_NAME_PARAMS);
    }
  }

  private CompletableFuture<Void> asyncReloadPluginConfig(
      final String pluginName, final BesuPlugin plugin) {
    LOG.info("Reloading plugin configuration: {}", pluginName);
    return plugin
        .reloadConfiguration()
        .whenComplete(
            (unused, throwable) -> {
              if (throwable != null) {
                LOG.error("Failed to reload plugin {}", pluginName, throwable);
              } else {
                LOG.info("Plugin {} has been reloaded successfully", pluginName);
              }
            });
  }

  private String resultToString(final Map.Entry<String, CompletableFuture<Void>> entry) {
    final var pluginName = entry.getKey();
    final var result = entry.getValue();
    if (result.isCompletedExceptionally()) {
      try {
        result.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return pluginName + ":failure(interrupted)";
      } catch (ExecutionException e) {
        return pluginName + ":failure(" + e.getCause().getMessage() + ")";
      }
    }
    return pluginName + ":success";
  }
}
