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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import com.fasterxml.jackson.annotation.JsonGetter;

public class TransactionPoolStatusResult {

  private final long pending;
  private final long queued;

  public TransactionPoolStatusResult(final long pending, final long queued) {
    this.pending = pending;
    this.queued = queued;
  }

  @JsonGetter(value = "pending")
  public String getPending() {
    return Quantity.create(pending);
  }

  @JsonGetter(value = "queued")
  public String getQueued() {
    return Quantity.create(queued);
  }
}
