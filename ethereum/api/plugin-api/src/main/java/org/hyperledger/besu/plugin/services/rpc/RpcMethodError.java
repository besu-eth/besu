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
package org.hyperledger.besu.plugin.services.rpc;

import java.util.Optional;

/**
 * The {@code RpcMethodError} interface defines the structure for RPC error handling within the
 * context of plugins. It provides methods to retrieve error code, message, and an optional data
 * decoder function.
 */
public interface RpcMethodError {

  /** The error code for all invalid params */
  static final int INVALID_PARAMS_ERROR_CODE = -32602;

  /**
   * Retrieves the error code associated with the RPC error.
   *
   * @return An integer representing the error code.
   */
  int getCode();

  /**
   * Retrieves the message associated with the RPC error.
   *
   * @return A {@code String} containing the error message.
   */
  String getMessage();

  /**
   * Some errors have additional data associated with them, that is possible to decode to provide a
   * more detailed error response.
   *
   * @param data the additional data to decode
   * @return an optional containing the decoded data if the error has it and the decoding is
   *     successful, otherwise empty.
   */
  default Optional<String> decodeData(final String data) {
    return Optional.empty();
  }
}
