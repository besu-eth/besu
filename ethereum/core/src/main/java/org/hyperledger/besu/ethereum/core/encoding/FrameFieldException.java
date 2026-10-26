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
package org.hyperledger.besu.ethereum.core.encoding;

import org.hyperledger.besu.ethereum.rlp.RLPException;

/**
 * A named field of an EIP-8141 frame transaction fell outside the range the EIP allows for it.
 *
 * <p>The EIP states these bounds as validity assertions over an already deserialised transaction
 * ({@code assert tx.nonce < 2**64}, {@code assert tx.fees.max_fee_per_gas < 2**256}), but a value
 * too wide for its field cannot be deserialised at all: the RLP reader rejects it, naming only the
 * kind of scalar it was reading. That is not enough to say which bound was broken, and the three
 * fee fields and a frame's value are all the same kind of scalar.
 *
 * <p>Carrying the failure as its own type lets the decoder name the field while leaving genuinely
 * structural RLP failures reported as such, so a caller can tell "this transaction declares a fee
 * above 2^256-1" from "this transaction's RLP is malformed".
 */
public class FrameFieldException extends RLPException {

  /**
   * Creates the exception.
   *
   * @param message names the field and the bound it broke
   */
  public FrameFieldException(final String message) {
    super(message);
  }
}
