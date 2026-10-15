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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.StartContext;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

public class PluginsReloadConfigurationTest {

  /** A plugin that counts its configuration reloads. */
  static class ReloadablePlugin implements BesuPlugin {
    int reloads;

    @Override
    public void register(final RegistrationContext context) {}

    @Override
    public void start(final StartContext context) {}

    @Override
    public void stop() {}

    @Override
    public CompletableFuture<Void> reloadConfiguration() {
      reloads++;
      return CompletableFuture.completedFuture(null);
    }
  }

  /** Holders of two plugins with the same simple class name. */
  static class First {
    static class Plugin extends ReloadablePlugin {}
  }

  static class Second {
    static class Plugin extends ReloadablePlugin {}
  }

  private final First.Plugin first = new First.Plugin();
  private final Second.Plugin second = new Second.Plugin();
  private final ReloadablePlugin other = new ReloadablePlugin();
  private final PluginsReloadConfiguration method =
      new PluginsReloadConfiguration(
          Map.of(
              First.Plugin.class.getName(), first,
              Second.Plugin.class.getName(), second,
              ReloadablePlugin.class.getName(), other));

  private JsonRpcResponse reload(final Object... params) {
    return method.response(
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", method.getName(), params)));
  }

  @Test
  public void reloadsThePluginWithTheGivenId() {
    assertThat(reload(First.Plugin.class.getName())).isInstanceOf(JsonRpcSuccessResponse.class);

    assertThat(first.reloads).isEqualTo(1);
    assertThat(second.reloads).isZero();
    assertThat(other.reloads).isZero();
  }

  @Test
  public void reloadsThePluginWithTheGivenSimpleClassNameWhenItNamesOnePlugin() {
    assertThat(reload("ReloadablePlugin")).isInstanceOf(JsonRpcSuccessResponse.class);

    assertThat(other.reloads).isEqualTo(1);
    assertThat(first.reloads).isZero();
    assertThat(second.reloads).isZero();
  }

  @Test
  public void rejectsASimpleClassNameNamingSeveralPluginsListingTheirIds() {
    final JsonRpcResponse response = reload("Plugin");

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    final JsonRpcError error = ((JsonRpcErrorResponse) response).getError();
    assertThat(error.getCode()).isEqualTo(RpcErrorType.INVALID_PLUGIN_NAME_PARAMS.getCode());
    assertThat(error.getMessage())
        .isEqualTo(
            "Plugin name Plugin matches several plugins, select one by its id: "
                + First.Plugin.class.getName()
                + ", "
                + Second.Plugin.class.getName());
    assertThat(first.reloads).isZero();
    assertThat(second.reloads).isZero();
  }

  @Test
  public void reportsANameThatNamesNoPlugin() {
    final JsonRpcResponse response = reload("Unknown");

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    assertThat(((JsonRpcErrorResponse) response).getErrorType())
        .isEqualTo(RpcErrorType.PLUGIN_NOT_FOUND);
  }

  @Test
  public void reloadsEveryPluginWhenNoNameIsGiven() {
    assertThat(reload()).isInstanceOf(JsonRpcSuccessResponse.class);

    assertThat(first.reloads).isEqualTo(1);
    assertThat(second.reloads).isEqualTo(1);
    assertThat(other.reloads).isEqualTo(1);
  }
}
