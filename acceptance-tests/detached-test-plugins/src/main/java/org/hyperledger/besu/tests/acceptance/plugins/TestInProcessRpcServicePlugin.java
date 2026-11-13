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
package org.hyperledger.besu.tests.acceptance.plugins;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.RpcEndpointService;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StateOverride;
import org.hyperledger.besu.datatypes.StateOverrideMap;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.plugin.services.rpc.PluginRpcRequest;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@AutoService(BesuPlugin.class)
public class TestInProcessRpcServicePlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(TestInProcessRpcServicePlugin.class);

  private RpcEndpointService rpcEndpointService;

  @CommandLine.Option(names = {"--plugin-test-set-min-gas-price"})
  long minGasPrice = -1;

  @Override
  public void register(final ServiceManager context) {
    final PicoCLIOptions cmdlineOptions =
        context
            .getService(PicoCLIOptions.class)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Failed to obtain PicoCLI options from the BesuContext"));

    cmdlineOptions.addPicoCLIOptions("test", this);

    rpcEndpointService =
        context
            .getService(RpcEndpointService.class)
            .orElseThrow(
                () ->
                    new RuntimeException(
                        "Failed to obtain RpcEndpointService from the BesuContext."));

    rpcEndpointService.registerRPCEndpoint("tests", "estimateGas", this::estimateGas);
  }

  @Override
  public void start() {
    LOG.info("TestInProcessRpcServicePlugin minGasPrice option: {}", minGasPrice);
    if (minGasPrice >= 0) {
      callSetMinGasPrice(minGasPrice);
    }
  }

  @Override
  public void stop() {}

  private void callSetMinGasPrice(final long minGasPrice) {
    LOG.info("Setting minGasPrice via in-process RPC service");
    final var minGasPriceWei = Wei.of(minGasPrice);
    final var resp =
        rpcEndpointService.call(
            "miner_setMinGasPrice", new Object[] {minGasPriceWei.toShortHexString()});
    LOG.info("miner_setMinGasPrice response: {}", resp);
    if (!resp.getType().equals(RpcResponseType.SUCCESS)) {
      throw new RuntimeException("Internal setMinGasPrice method failed: " + resp);
    }
  }

  private String estimateGas(final PluginRpcRequest request) {
    final Object[] params = request.getParams();
    final Address from = Address.fromHexString(params[0].toString());
    final Address to = Address.fromHexString(params[1].toString());
    final boolean strict = Boolean.parseBoolean(params[2].toString());
    final boolean zeroBalance = Boolean.parseBoolean(params[3].toString());

    final var callParam =
        ImmutableCallParameter.builder().sender(from).to(to).strict(strict).build();

    final Object[] callParams;
    if (zeroBalance) {
      final var overrides = new StateOverrideMap();
      overrides.put(from, StateOverride.builder().withBalance(Wei.ZERO).build());
      callParams = new Object[] {callParam, "pending", overrides};
    } else {
      callParams = new Object[] {callParam, "pending"};
    }

    final var resp = rpcEndpointService.call("eth_estimateGas", callParams);

    if (resp.getType() == RpcResponseType.ERROR) {
      final var err = (JsonRpcError) resp.getResult();
      return resp.getType() + " " + err.getCode() + " " + err.getMessage();
    }
    return resp.getType() + " " + resp.getResult();
  }
}
