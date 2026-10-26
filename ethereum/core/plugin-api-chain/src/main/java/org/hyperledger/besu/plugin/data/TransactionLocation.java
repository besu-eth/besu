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
package org.hyperledger.besu.plugin.data;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.Unstable;

/** The location of a transaction in a block. */
@Unstable
public interface TransactionLocation {

  /**
   * Get the hash of the block that includes the transaction
   *
   * @return the block hash
   */
  Hash getBlockHash();

  /**
   * Get the index of the transaction in the block
   *
   * @return the transaction index
   */
  int getTransactionIndex();
}
