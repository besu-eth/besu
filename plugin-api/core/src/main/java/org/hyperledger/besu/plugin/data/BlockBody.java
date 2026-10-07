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
package org.hyperledger.besu.plugin.data;

import org.hyperledger.besu.datatypes.Transaction;

import java.util.List;
import java.util.Optional;

/**
 * The parts of a Block not in the {@link BlockHeader}, information corresponding to the comprised
 * transactions in {@link #getTransactions()}, and a set of other block headers in {@link
 * #getOmmers()}, as defined in the <a
 * href="https://ethereum.github.io/yellowpaper/paper.pdf">Ethereum Yellow Paper</a>.
 */
public interface BlockBody {
  /**
   * Returns the list of transactions of the block.
   *
   * @return The list of transactions of the block.
   */
  List<? extends Transaction> getTransactions();

  /**
   * Returns the list of ommers of the block.
   *
   * @return The list of ommers of the block.
   */
  List<? extends BlockHeader> getOmmers();

  /**
   * Returns the list of withdrawals of the block.
   *
   * @return The list of withdrawals of the block.
   */
  Optional<? extends List<? extends Withdrawal>> getWithdrawals();
}
