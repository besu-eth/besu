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
package org.hyperledger.besu.ethereum.mainnet.forkstatechange;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import com.google.common.io.Resources;
import io.vertx.core.json.JsonArray;

/**
 * The DAO refund (EIP-779): moves the balance of every account in {@code daoAddresses.json} to the
 * refund contract.
 */
public final class DaoRefundStateChange implements ForkStateChange {

  private static final Address DAO_REFUND_CONTRACT_ADDRESS =
      Address.fromHexString("0xbf4ed7b27f1d666546e30d74d50d173d20bca754");

  private static final List<Address> DAO_ADDRESSES = loadDaoAddresses();

  @Override
  public void apply(final WorldUpdater updater) {
    final MutableAccount daoRefundContract = updater.getOrCreate(DAO_REFUND_CONTRACT_ADDRESS);
    for (final Address address : DAO_ADDRESSES) {
      final MutableAccount account = updater.getOrCreate(address);
      final Wei balance = account.getBalance();
      account.decrementBalance(balance);
      daoRefundContract.incrementBalance(balance);
    }
  }

  private static List<Address> loadDaoAddresses() {
    try {
      final JsonArray json =
          new JsonArray(
              Resources.toString(
                  Objects.requireNonNull(
                      DaoRefundStateChange.class.getResource("/daoAddresses.json")),
                  StandardCharsets.UTF_8));
      return IntStream.range(0, json.size())
          .mapToObj(json::getString)
          .map(Address::fromHexString)
          .toList();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
