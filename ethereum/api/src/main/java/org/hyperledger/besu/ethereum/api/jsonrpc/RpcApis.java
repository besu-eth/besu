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
package org.hyperledger.besu.ethereum.api.jsonrpc;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum RpcApis {
  ETH,
  DEBUG,
  MINER,
  NET,
  PERM,
  WEB3,
  ADMIN,
  TXPOOL,
  TRACE,
  PLUGINS,
  IBFT,
  ENGINE,
  QBFT,
  TESTING;

  public static final List<String> DEFAULT_RPC_APIS = Arrays.asList("ETH", "NET", "WEB3");

  @SuppressWarnings("unused")
  public static final List<RpcApis> ALL_JSON_RPC_APIS =
      Arrays.asList(ETH, DEBUG, MINER, NET, PERM, WEB3, ADMIN, TXPOOL, TRACE, PLUGINS);

  public static final List<String> VALID_APIS =
      Stream.of(RpcApis.values()).map(RpcApis::name).collect(Collectors.toList());
}
