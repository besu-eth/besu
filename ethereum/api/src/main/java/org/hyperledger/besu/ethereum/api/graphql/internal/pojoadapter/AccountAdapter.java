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
package org.hyperledger.besu.ethereum.api.graphql.internal.pojoadapter;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.AccountState;

import java.util.Optional;

import graphql.schema.DataFetchingEnvironment;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * The AccountAdapter class extends the AdapterBase class. It provides methods to get the account
 * details such as address, balance, transaction count, code, and storage.
 */
@SuppressWarnings("unused") // reflected by GraphQL
public class AccountAdapter extends AdapterBase {

  private final Optional<Account> account;
  private final Address address;
  private final Optional<Long> blockNumber;

  /**
   * Constructs a new AccountAdapter with the given Account.
   *
   * @param account the Account to be adapted
   */
  public AccountAdapter(final Account account) {
    this(account == null ? null : account.getAddress(), account, Optional.empty());
  }

  /**
   * Constructs a new AccountAdapter with the given Account and block number.
   *
   * @param account the Account to be adapted
   * @param blockNumber the block number associated with the account
   */
  public AccountAdapter(final Account account, final Optional<Long> blockNumber) {
    this(account == null ? null : account.getAddress(), account, blockNumber);
  }

  /**
   * Constructs a new AccountAdapter with the given address and Account.
   *
   * @param address the address of the account
   * @param account the Account to be adapted
   */
  public AccountAdapter(final Address address, final Account account) {
    this(address, account, Optional.empty());
  }

  /**
   * Constructs a new AccountAdapter with the given address, Account, and block number.
   *
   * @param address the address of the account
   * @param account the Account to be adapted
   * @param blockNumber the block number associated with the account
   */
  public AccountAdapter(
      final Address address, final Account account, final Optional<Long> blockNumber) {
    this.account = Optional.ofNullable(account);
    this.address = address;
    this.blockNumber = blockNumber;
  }

  /**
   * Returns the address of the account.
   *
   * @return the address of the account
   */
  public Address getAddress() {
    return address;
  }

  /**
   * Returns the balance of the account.
   *
   * @return the balance of the account
   */
  public Wei getBalance() {
    return account.map(AccountState::getBalance).orElse(Wei.ZERO);
  }

  /**
   * Returns the transaction count of the account.
   *
   * @return the transaction count of the account
   */
  public Long getTransactionCount() {
    return account.map(AccountState::getNonce).orElse(0L);
  }

  /**
   * Returns the code of the account.
   *
   * @param environment the DataFetchingEnvironment
   * @return the code of the account
   */
  public Bytes getCode(final DataFetchingEnvironment environment) {

    if (account.get() instanceof BonsaiAccount) {
      final BlockchainQueries query = getBlockchainQueries(environment);
      return query
          .getAndMapWorldState(
              blockNumber.orElse(query.headBlockNumber()),
              ws -> Optional.of(ws.get(account.get().getAddress()).getCode()))
          .get();
    } else {
      return account.map(AccountState::getCode).orElse(Bytes.EMPTY);
    }
  }

  /**
   * Returns the storage of the account.
   *
   * @param environment the DataFetchingEnvironment
   * @return the storage of the account
   */
  public Bytes32 getStorage(final DataFetchingEnvironment environment) {
    final BlockchainQueries query = getBlockchainQueries(environment);
    final Bytes32 slot = environment.getArgument("slot");

    if (account.get() instanceof BonsaiAccount) {
      return query
          .getAndMapWorldState(
              blockNumber.orElse(query.headBlockNumber()),
              ws -> Optional.of((Bytes32) ws.get(address).getStorageValue(UInt256.fromBytes(slot))))
          .get();
    } else {
      return account
          .map(a -> (Bytes32) a.getStorageValue(UInt256.fromBytes(slot)))
          .orElse(Bytes32.ZERO);
    }
  }
}
