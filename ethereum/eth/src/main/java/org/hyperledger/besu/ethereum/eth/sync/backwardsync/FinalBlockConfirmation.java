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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Arrays;

public interface FinalBlockConfirmation {
  boolean ancestorHeaderReached(final BlockHeader firstHeader);

  static FinalBlockConfirmation genesisConfirmation(final MutableBlockchain blockchain) {
    final BlockHeader genesisBlockHeader = blockchain.getBlockHeader(0).orElseThrow();
    return firstHeader -> {
      if (firstHeader.getNumber() > 0) {
        return false;
      }

      if (!genesisBlockHeader.getHash().equals(firstHeader.getHash())) {
        throw new BackwardSyncException(
            "Should have reached header "
                + genesisBlockHeader.toLogString()
                + " but reached "
                + firstHeader.toLogString());
      }
      return true;
    };
  }

  static FinalBlockConfirmation ancestorConfirmation(final BackwardSyncContext context) {
    return firstHeader ->
        context
            .getProtocolContext()
            .getBlockchain()
            .getBlockHeader(firstHeader.getParentHash())
            .map(context::isAlreadyImported)
            .orElse(false);
  }

  static FinalBlockConfirmation confirmationChain(final FinalBlockConfirmation... confirmations) {
    return firstHeader -> {
      return Arrays.stream(confirmations)
          .reduce(
              false,
              (aBoolean, confirmation2) ->
                  aBoolean || confirmation2.ancestorHeaderReached(firstHeader),
              (aBoolean, aBoolean2) -> aBoolean || aBoolean2);
    };
  }
}
