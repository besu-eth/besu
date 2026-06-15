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
package org.hyperledger.besu.tests.acceptance.dsl.transaction.debug;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.tests.acceptance.dsl.transaction.NodeRequests;
import org.hyperledger.besu.tests.acceptance.dsl.transaction.Transaction;

import java.io.IOException;
import java.util.List;

import org.web3j.protocol.core.Request;

public class DebugGetModifiedAccounts implements Transaction<List<String>> {

  private final String method;
  private final List<String> params;

  public DebugGetModifiedAccounts(final String method, final List<String> params) {
    this.method = method;
    this.params = params;
  }

  @Override
  public List<String> execute(final NodeRequests node) {
    try {
      final Request<?, DebugRequestFactory.GetModifiedAccountsResponse> request =
          switch (method) {
            case "debug_getModifiedAccountsByNumber" ->
                node.debug().getModifiedAccountsByNumber(params);
            case "debug_getModifiedAccountsByHash" ->
                node.debug().getModifiedAccountsByHash(params);
            default -> throw new IllegalArgumentException("Unsupported debug method: " + method);
          };
      final DebugRequestFactory.GetModifiedAccountsResponse response = request.send();
      assertThat(response).isNotNull();
      assertThat(response.hasError()).isFalse();
      return response.getResult();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
