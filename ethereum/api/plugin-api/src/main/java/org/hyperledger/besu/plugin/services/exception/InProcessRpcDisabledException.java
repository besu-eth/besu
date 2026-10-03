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
package org.hyperledger.besu.plugin.services.exception;

/**
 * Thrown by {@link org.hyperledger.besu.plugin.services.InProcessRpcService#call(String, Object[])}
 * when in-process RPC is disabled on this node.
 */
public class InProcessRpcDisabledException extends IllegalStateException {

  /** Creates the exception. */
  public InProcessRpcDisabledException() {
    super(
        "In-process RPC is disabled on this node; enable it with --Xin-process-rpc-enabled and"
            + " select the namespaces with --Xin-process-rpc-apis");
  }
}
