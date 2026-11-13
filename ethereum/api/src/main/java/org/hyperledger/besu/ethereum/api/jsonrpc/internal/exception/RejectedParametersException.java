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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;

/**
 * Thrown when a request carries a parameter that the method does not support. Unlike {@link
 * InvalidJsonRpcParameters}, whose message is not returned to the caller, methods that catch this
 * exception return its message in the error response.
 */
public class RejectedParametersException extends InvalidJsonRpcParameters {

  public RejectedParametersException(final String message, final RpcErrorType rpcErrorType) {
    super(message, rpcErrorType);
  }
}
