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
package org.hyperledger.besu.evm.worldstate;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import java.util.Collection;

import org.apache.tuweni.bytes.Bytes;

/** Settles the accounts marked for self-destruction once a transaction has executed. */
public final class SelfDestructSettlement {

  private SelfDestructSettlement() {}

  /**
   * Applies the fork's self-destruct rules to the given accounts.
   *
   * <p>Before EIP-8246 each account is deleted outright. Under EIP-8246 each account is cleared
   * (nonce reset, code and storage removed) but keeps its balance; EIP-161 state clearing (via
   * {@link WorldUpdater#clearAccountsThatAreEmpty()}) then removes any account left with a zero
   * balance.
   *
   * @param worldUpdater the world updater holding the accounts
   * @param selfDestructs the addresses marked for self-destruction
   * @param gasCalculator the gas calculator of the active fork
   */
  public static void settle(
      final WorldUpdater worldUpdater,
      final Collection<Address> selfDestructs,
      final GasCalculator gasCalculator) {
    if (gasCalculator.isSelfDestructBalancePreserved()) {
      selfDestructs.forEach(
          address -> {
            final MutableAccount account = worldUpdater.getAccount(address);
            if (account != null) {
              account.setNonce(0L);
              account.setCode(Bytes.EMPTY);
              account.clearStorage();
            }
          });
    } else {
      selfDestructs.forEach(worldUpdater::deleteAccount);
    }
  }
}
