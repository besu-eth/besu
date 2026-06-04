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
package org.hyperledger.besu.ethereum.api.jsonrpc.health;

import io.netty.handler.codec.http.HttpResponseStatus;

public enum HealthCheckResult {
  HEALTHY(HttpResponseStatus.OK.code(), "UP"),
  UNHEALTHY(HttpResponseStatus.SERVICE_UNAVAILABLE.code(), "DOWN"),
  BAD_REQUEST(HttpResponseStatus.BAD_REQUEST.code(), "BAD_REQUEST");

  private final int statusCode;
  private final String statusText;

  HealthCheckResult(final int statusCode, final String statusText) {
    this.statusCode = statusCode;
    this.statusText = statusText;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public String getStatusText() {
    return statusText;
  }
}
