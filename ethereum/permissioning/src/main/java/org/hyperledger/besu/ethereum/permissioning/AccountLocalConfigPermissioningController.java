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
package org.hyperledger.besu.ethereum.permissioning;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.ethereum.permissioning.AllowlistPersistor.ALLOWLIST_TYPE;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.permissioning.TransactionPermissioningProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccountLocalConfigPermissioningController implements TransactionPermissioningProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(AccountLocalConfigPermissioningController.class);

  private static final int ACCOUNT_BYTES_SIZE = 20;
  private LocalPermissioningConfiguration configuration;
  private List<String> accountAllowlist = new ArrayList<>();
  private final AllowlistPersistor allowlistPersistor;

  private final Counter checkCounter;
  private final Counter checkCounterPermitted;
  private final Counter checkCounterUnpermitted;

  public AccountLocalConfigPermissioningController(
      final LocalPermissioningConfiguration configuration, final MetricsSystem metricsSystem) {
    this(
        configuration,
        new AllowlistPersistor(
            Objects.requireNonNull(configuration.getAccountPermissioningConfigFilePath())),
        metricsSystem);
  }

  public AccountLocalConfigPermissioningController(
      final LocalPermissioningConfiguration configuration,
      final AllowlistPersistor allowlistPersistor,
      final MetricsSystem metricsSystem) {
    this.configuration = configuration;
    this.allowlistPersistor = allowlistPersistor;
    readAccountsFromConfig(configuration);
    this.checkCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.PERMISSIONING,
            "account_local_check_count",
            "Number of times the account local permissioning provider has been checked");
    this.checkCounterPermitted =
        metricsSystem.createCounter(
            BesuMetricCategory.PERMISSIONING,
            "account_local_check_count_permitted",
            "Number of times the account local permissioning provider has been checked and returned permitted");
    this.checkCounterUnpermitted =
        metricsSystem.createCounter(
            BesuMetricCategory.PERMISSIONING,
            "account_local_check_count_unpermitted",
            "Number of times the account local permissioning provider has been checked and returned unpermitted");
  }

  private void readAccountsFromConfig(final LocalPermissioningConfiguration configuration) {
    if (configuration != null && configuration.isAccountAllowlistEnabled()) {
      if (!configuration.getAccountAllowlist().isEmpty()) {
        AllowlistOperationResult result = addAccounts(configuration.getAccountAllowlist());
        if (result != AllowlistOperationResult.SUCCESS) {
          throw new IllegalStateException(
              String.format(
                  "Error reloading permissions file. Invalid accounts allowlist, validation failed due to \"%s\"",
                  result));
        }
      }
    }
  }

  public AllowlistOperationResult addAccounts(final List<String> accounts) {
    final List<String> normalizedAccounts = normalizeAccounts(accounts);
    final AllowlistOperationResult inputValidationResult = inputValidation(normalizedAccounts);
    if (inputValidationResult != AllowlistOperationResult.SUCCESS) {
      return inputValidationResult;
    }

    boolean inputHasExistingAccount =
        normalizedAccounts.stream().anyMatch(accountAllowlist::contains);
    if (inputHasExistingAccount) {
      return AllowlistOperationResult.ERROR_EXISTING_ENTRY;
    }

    final List<String> oldAllowlist = new ArrayList<>(this.accountAllowlist);
    this.accountAllowlist.addAll(normalizedAccounts);
    try {
      verifyConfigurationFileState(oldAllowlist);
      updateConfigurationFile(accountAllowlist);
      verifyConfigurationFileState(accountAllowlist);
    } catch (IOException e) {
      revertState(oldAllowlist);
      return AllowlistOperationResult.ERROR_ALLOWLIST_PERSIST_FAIL;
    } catch (AllowlistFileSyncException e) {
      return AllowlistOperationResult.ERROR_ALLOWLIST_FILE_SYNC;
    }
    return AllowlistOperationResult.SUCCESS;
  }

  public AllowlistOperationResult removeAccounts(final List<String> accounts) {
    final List<String> normalizedAccounts = normalizeAccounts(accounts);
    final AllowlistOperationResult inputValidationResult = inputValidation(normalizedAccounts);
    if (inputValidationResult != AllowlistOperationResult.SUCCESS) {
      return inputValidationResult;
    }

    if (!accountAllowlist.containsAll(normalizedAccounts)) {
      return AllowlistOperationResult.ERROR_ABSENT_ENTRY;
    }

    final List<String> oldAllowlist = new ArrayList<>(this.accountAllowlist);

    this.accountAllowlist.removeAll(normalizedAccounts);
    try {
      verifyConfigurationFileState(oldAllowlist);
      updateConfigurationFile(accountAllowlist);
      verifyConfigurationFileState(accountAllowlist);
    } catch (IOException e) {
      revertState(oldAllowlist);
      return AllowlistOperationResult.ERROR_ALLOWLIST_PERSIST_FAIL;
    } catch (AllowlistFileSyncException e) {
      return AllowlistOperationResult.ERROR_ALLOWLIST_FILE_SYNC;
    }
    return AllowlistOperationResult.SUCCESS;
  }

  private AllowlistOperationResult inputValidation(final List<String> accounts) {
    if (accounts == null || accounts.isEmpty()) {
      return AllowlistOperationResult.ERROR_EMPTY_ENTRY;
    }

    if (containsInvalidAccount(accounts)) {
      return AllowlistOperationResult.ERROR_INVALID_ENTRY;
    }

    if (inputHasDuplicates(accounts)) {
      return AllowlistOperationResult.ERROR_DUPLICATED_ENTRY;
    }

    return AllowlistOperationResult.SUCCESS;
  }

  private void verifyConfigurationFileState(final Collection<String> oldAccounts)
      throws IOException, AllowlistFileSyncException {
    allowlistPersistor.verifyConfigFileMatchesState(ALLOWLIST_TYPE.ACCOUNTS, oldAccounts);
  }

  private void updateConfigurationFile(final Collection<String> accounts) throws IOException {
    allowlistPersistor.updateConfig(ALLOWLIST_TYPE.ACCOUNTS, accounts);
  }

  private void revertState(final List<String> accountAllowlist) {
    this.accountAllowlist = accountAllowlist;
  }

  private boolean inputHasDuplicates(final List<String> accounts) {
    return !accounts.stream().allMatch(new HashSet<>()::add);
  }

  public boolean contains(final String account) {
    return accountAllowlist.stream().anyMatch(a -> a.equalsIgnoreCase(account));
  }

  public List<String> getAccountAllowlist() {
    return new ArrayList<>(accountAllowlist);
  }

  private boolean containsInvalidAccount(final List<String> accounts) {
    return !accounts.stream()
        .allMatch(AccountLocalConfigPermissioningController::isValidAccountString);
  }

  static boolean isValidAccountString(final String account) {
    try {
      if (account == null || !account.startsWith("0x")) {
        return false;
      }
      Bytes bytes = Bytes.fromHexString(account);
      return bytes.size() == ACCOUNT_BYTES_SIZE;
    } catch (NullPointerException | IndexOutOfBoundsException | IllegalArgumentException e) {
      return false;
    }
  }

  public synchronized void reload() throws RuntimeException {
    final ArrayList<String> currentAccountsList = new ArrayList<>(accountAllowlist);
    accountAllowlist.clear();

    try {
      final LocalPermissioningConfiguration updatedConfig =
          PermissioningConfigurationBuilder.permissioningConfiguration(
              configuration.isNodeAllowlistEnabled(),
              configuration.getEnodeDnsConfiguration(),
              configuration.getNodePermissioningConfigFilePath(),
              configuration.isAccountAllowlistEnabled(),
              configuration.getAccountPermissioningConfigFilePath());
      readAccountsFromConfig(updatedConfig);
      configuration = updatedConfig;
    } catch (Exception e) {
      accountAllowlist.clear();
      accountAllowlist.addAll(currentAccountsList);
      throw new IllegalStateException(
          "Error reloading permissions file. In-memory accounts allowlist will be reverted to previous valid configuration",
          e);
    }
  }

  private List<String> normalizeAccounts(final List<String> accounts) {
    if (accounts != null) {
      return accounts.parallelStream().map(String::toLowerCase).collect(Collectors.toList());
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public boolean isPermitted(final Transaction transaction) {
    final Hash transactionHash = transaction.getHash();
    final Address sender = transaction.getSender();

    LOG.trace("Account permissioning - Local Config: Checking transaction {}", transactionHash);

    this.checkCounter.inc();
    if (sender == null) {
      this.checkCounterUnpermitted.inc();
      LOG.trace(
          "Account permissioning - Local Config: Rejected transaction {} without sender",
          transactionHash);
      return false;
    } else {
      if (contains(sender.toString())) {
        this.checkCounterPermitted.inc();
        LOG.trace(
            "Account permissioning - Local Config: Permitted transaction {} from {}",
            transactionHash,
            sender);
        return true;
      } else {
        this.checkCounterUnpermitted.inc();
        LOG.trace(
            "Account permissioning - Local Config: Rejected transaction {} from {}",
            transactionHash,
            sender);
        return false;
      }
    }
  }
}
