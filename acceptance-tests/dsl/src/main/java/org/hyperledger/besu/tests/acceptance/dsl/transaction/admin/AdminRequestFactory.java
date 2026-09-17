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
package org.hyperledger.besu.tests.acceptance.dsl.transaction.admin;

import java.net.URI;
import java.util.Collections;
import java.util.Map;

import org.web3j.protocol.Web3jService;
import org.web3j.protocol.core.Request;
import org.web3j.protocol.core.Response;

public class AdminRequestFactory {

  public static class AdminAddPeerResponse extends Response<Boolean> {}

  public static class AdminNodeInfoResponse extends Response<Map<String, Object>> {}

  private final Web3jService web3jService;

  public AdminRequestFactory(final Web3jService web3jService) {
    this.web3jService = web3jService;
  }

  public Request<?, AdminAddPeerResponse> adminAddPeer(final URI enodeAddress) {
    return new Request<>(
        "admin_addPeer",
        Collections.singletonList(enodeAddress.toASCIIString()),
        web3jService,
        AdminAddPeerResponse.class);
  }

  public Request<?, AdminNodeInfoResponse> adminNodeInfo() {
    return new Request<>(
        "admin_nodeInfo", Collections.emptyList(), web3jService, AdminNodeInfoResponse.class);
  }
}
