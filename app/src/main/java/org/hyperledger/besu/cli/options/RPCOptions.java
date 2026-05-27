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
package org.hyperledger.besu.cli.options;

import org.hyperledger.besu.ethereum.api.handlers.TimeoutOptions;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;

import picocli.CommandLine;

/** The Rpc Cli options. */
public class RPCOptions {

  @CommandLine.Option(
      hidden = true,
      names = {"--Xhttp-timeout-seconds"},
      description = "HTTP timeout in seconds (default: ${DEFAULT-VALUE})")
  private final Long httpTimeoutSec = TimeoutOptions.defaultOptions().getTimeoutSeconds();

  @CommandLine.Option(
      hidden = true,
      names = {"--Xhttp-streaming-timeout-seconds"},
      description =
          "HTTP timeout in seconds for streaming methods like debug_traceBlock (default: ${DEFAULT-VALUE})")
  private final Long httpStreamingTimeoutSec =
      JsonRpcConfiguration.DEFAULT_HTTP_STREAMING_TIMEOUT_SEC;

  @CommandLine.Option(
      hidden = true,
      names = {"--Xws-timeout-seconds"},
      description = "Web socket timeout in seconds (default: ${DEFAULT-VALUE})")
  private final Long wsTimeoutSec = TimeoutOptions.defaultOptions().getTimeoutSeconds();

  /** Default Constructor. */
  RPCOptions() {}

  /**
   * Create rpc options.
   *
   * @return the rpc options
   */
  public static RPCOptions create() {
    return new RPCOptions();
  }

  /**
   * Gets http timeout sec.
   *
   * @return the http timeout sec
   */
  public Long getHttpTimeoutSec() {
    return httpTimeoutSec;
  }

  /**
   * Gets http streaming timeout sec.
   *
   * @return the http streaming timeout sec
   */
  public Long getHttpStreamingTimeoutSec() {
    return httpStreamingTimeoutSec;
  }

  /**
   * Gets WebSocket timeout sec.
   *
   * @return the WebSocket timeout sec
   */
  public Long getWsTimeoutSec() {
    return wsTimeoutSec;
  }
}
