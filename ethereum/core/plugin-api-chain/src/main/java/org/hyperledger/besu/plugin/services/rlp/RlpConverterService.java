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
package org.hyperledger.besu.plugin.services.rlp;

import org.hyperledger.besu.plugin.data.BlockBody;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.data.TransactionReceipt;
import org.hyperledger.besu.plugin.services.BesuService;

import org.apache.tuweni.bytes.Bytes;

/** RLP serialization and deserialization service. */
public interface RlpConverterService extends BesuService {

  /**
   * Builds a block header from RLP.
   *
   * @param rlp the RLP to build the block header from.
   * @return the block header.
   */
  BlockHeader buildHeaderFromRlp(final Bytes rlp);

  /**
   * Builds a block body from RLP.
   *
   * @param rlp the RLP to build the block body from.
   * @return the block body.
   */
  BlockBody buildBodyFromRlp(final Bytes rlp);

  /**
   * Builds a transaction receipt from RLP.
   *
   * @param rlp the RLP to build the transaction receipt from.
   * @return the transaction receipt.
   */
  TransactionReceipt buildReceiptFromRlp(final Bytes rlp);

  /**
   * RLP encodes a block header.
   *
   * @param blockHeader the block header to build RLP from.
   * @return the RLP.
   */
  Bytes buildRlpFromHeader(final BlockHeader blockHeader);

  /**
   * RLP encodes a block body.
   *
   * @param blockBody the block body to build RLP from.
   * @return the RLP.
   */
  Bytes buildRlpFromBody(final BlockBody blockBody);

  /**
   * RLP encodes a transaction receipt.
   *
   * @param receipt the transaction receipt to build RLP from.
   * @return the RLP.
   */
  Bytes buildRlpFromReceipt(final TransactionReceipt receipt);
}
