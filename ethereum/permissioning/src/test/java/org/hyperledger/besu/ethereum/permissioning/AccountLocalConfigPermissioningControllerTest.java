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

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AccountLocalConfigPermissioningControllerTest {

  private AccountLocalConfigPermissioningController controller;
  @Mock private LocalPermissioningConfiguration permissioningConfig;
  @Mock private AllowlistPersistor allowlistPersistor;
  @Mock private MetricsSystem metricsSystem;
  @Mock private Counter checkCounter;
  @Mock private Counter checkPermittedCounter;
  @Mock private Counter checkUnpermittedCounter;

  @BeforeEach
  public void before() {

    when(metricsSystem.createCounter(
            BesuMetricCategory.PERMISSIONING,
            "account_local_check_count",
            "Number of times the account local permissioning provider has been checked"))
        .thenReturn(checkCounter);

    when(metricsSystem.createCounter(
            BesuMetricCategory.PERMISSIONING,
            "account_local_check_count_permitted",
            "Number of times the account local permissioning provider has been checked and returned permitted"))
        .thenReturn(checkPermittedCounter);

    when(metricsSystem.createCounter(
            BesuMetricCategory.PERMISSIONING,
            "account_local_check_count_unpermitted",
            "Number of times the account local permissioning provider has been checked and returned unpermitted"))
        .thenReturn(checkUnpermittedCounter);

    controller =
        new AccountLocalConfigPermissioningController(
            permissioningConfig, allowlistPersistor, metricsSystem);
  }

  @Test
  public void whenPermConfigHasAccountsShouldAddAllAccountsToAllowlist() {
    when(permissioningConfig.isAccountAllowlistEnabled()).thenReturn(true);
    when(permissioningConfig.getAccountAllowlist())
        .thenReturn(singletonList("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));
    controller =
        new AccountLocalConfigPermissioningController(
            permissioningConfig, allowlistPersistor, metricsSystem);

    assertThat(controller.getAccountAllowlist())
        .contains("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73");
  }

  @Test
  public void missingAccountPermissioningFileIsInvalidConfigurationState() {
    assertThatThrownBy(
            () -> new AccountLocalConfigPermissioningController(permissioningConfig, metricsSystem))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Account permissioning config file path is required when account permissioning is enabled");
  }

  @Test
  public void whenLoadingAccountsFromConfigShouldNormalizeAccountsToLowerCase() {
    when(permissioningConfig.isAccountAllowlistEnabled()).thenReturn(true);
    when(permissioningConfig.getAccountAllowlist())
        .thenReturn(singletonList("0xFE3B557E8Fb62b89F4916B721be55cEb828dBd73"));
    controller =
        new AccountLocalConfigPermissioningController(
            permissioningConfig, allowlistPersistor, metricsSystem);

    assertThat(controller.getAccountAllowlist())
        .containsExactly("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73");
  }

  @Test
  public void whenLoadingDuplicateAccountsFromConfigShouldThrowError() {
    when(permissioningConfig.isAccountAllowlistEnabled()).thenReturn(true);
    when(permissioningConfig.getAccountAllowlist())
        .thenReturn(
            List.of(
                "0xcb88953e60948e3a76fa658d65b7c2d5043c6409",
                "0xdd76406b124f9e3ae9fbeb47e4d8dc0ab143902d",
                "0x432132e8561785c33afe931762cf8eeb9c80e3ad",
                "0xcb88953e60948e3a76fa658d65b7c2d5043c6409"));

    assertThrows(
        IllegalStateException.class,
        () -> {
          controller =
              new AccountLocalConfigPermissioningController(
                  permissioningConfig, allowlistPersistor, metricsSystem);
        });
  }

  @Test
  public void whenLoadingInvalidAccountsFromConfigShouldThrowError() {
    when(permissioningConfig.isAccountAllowlistEnabled()).thenReturn(true);
    when(permissioningConfig.getAccountAllowlist()).thenReturn(List.of("0x0", "0xzxy"));

    assertThrows(
        IllegalStateException.class,
        () -> {
          controller =
              new AccountLocalConfigPermissioningController(
                  permissioningConfig, allowlistPersistor, metricsSystem);
        });
  }

  @Test
  public void whenPermConfigContainsEmptyListOfAccountsContainsShouldReturnFalse() {
    when(permissioningConfig.isAccountAllowlistEnabled()).thenReturn(true);
    when(permissioningConfig.getAccountAllowlist()).thenReturn(new ArrayList<>());
    controller =
        new AccountLocalConfigPermissioningController(
            permissioningConfig, allowlistPersistor, metricsSystem);

    assertThat(controller.contains("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73")).isFalse();
  }

  @Test
  public void addAccountsWithInvalidAccountShouldReturnInvalidEntryResult() {
    AllowlistOperationResult addResult = controller.addAccounts(Arrays.asList("0x0"));

    assertThat(addResult).isEqualTo(AllowlistOperationResult.ERROR_INVALID_ENTRY);
    assertThat(controller.getAccountAllowlist()).isEmpty();
  }

  @Test
  public void addExistingAccountShouldReturnExistingEntryResult() {
    controller.addAccounts(Arrays.asList("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));
    AllowlistOperationResult addResult =
        controller.addAccounts(Arrays.asList("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));

    assertThat(addResult).isEqualTo(AllowlistOperationResult.ERROR_EXISTING_ENTRY);
    assertThat(controller.getAccountAllowlist())
        .containsExactly("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73");
  }

  @Test
  public void addExistingAccountWithDifferentCasingShouldReturnExistingEntryResult() {
    controller.addAccounts(Arrays.asList("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));
    AllowlistOperationResult addResult =
        controller.addAccounts(Arrays.asList("0xFE3B557E8Fb62b89F4916B721be55cEb828dBd73"));

    assertThat(addResult).isEqualTo(AllowlistOperationResult.ERROR_EXISTING_ENTRY);
    assertThat(controller.getAccountAllowlist())
        .containsExactly("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73");
  }

  @Test
  public void addValidAccountsShouldReturnSuccessResult() {
    AllowlistOperationResult addResult =
        controller.addAccounts(Arrays.asList("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));

    assertThat(addResult).isEqualTo(AllowlistOperationResult.SUCCESS);
    assertThat(controller.getAccountAllowlist())
        .containsExactly("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73");
  }

  @Test
  public void addAccountsShouldAddAccountNormalizedToLowerCase() {
    AllowlistOperationResult addResult =
        controller.addAccounts(Arrays.asList("0xFE3B557E8Fb62b89F4916B721be55cEb828dBd73"));

    assertThat(addResult).isEqualTo(AllowlistOperationResult.SUCCESS);
    assertThat(controller.getAccountAllowlist())
        .containsExactly("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73");
  }

  @Test
  public void removeExistingAccountShouldReturnSuccessResult() {
    controller.addAccounts(Arrays.asList("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));

    AllowlistOperationResult removeResult =
        controller.removeAccounts(Arrays.asList("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));

    assertThat(removeResult).isEqualTo(AllowlistOperationResult.SUCCESS);
    assertThat(controller.getAccountAllowlist()).isEmpty();
  }

  @Test
  public void removeAccountShouldNormalizeAccountToLowerCAse() {
    controller.addAccounts(Arrays.asList("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));

    AllowlistOperationResult removeResult =
        controller.removeAccounts(Arrays.asList("0xFE3B557E8Fb62b89F4916B721be55cEb828dBd73"));

    assertThat(removeResult).isEqualTo(AllowlistOperationResult.SUCCESS);
    assertThat(controller.getAccountAllowlist()).isEmpty();
  }

  @Test
  public void removeAbsentAccountShouldReturnAbsentEntryResult() {
    AllowlistOperationResult removeResult =
        controller.removeAccounts(Arrays.asList("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));

    assertThat(removeResult).isEqualTo(AllowlistOperationResult.ERROR_ABSENT_ENTRY);
    assertThat(controller.getAccountAllowlist()).isEmpty();
  }

  @Test
  public void removeInvalidAccountShouldReturnInvalidEntryResult() {
    AllowlistOperationResult removeResult = controller.removeAccounts(Arrays.asList("0x0"));

    assertThat(removeResult).isEqualTo(AllowlistOperationResult.ERROR_INVALID_ENTRY);
    assertThat(controller.getAccountAllowlist()).isEmpty();
  }

  @Test
  public void addDuplicatedAccountShouldReturnDuplicatedEntryResult() {
    AllowlistOperationResult addResult =
        controller.addAccounts(
            Arrays.asList(
                "0xfe3b557e8fb62b89f4916b721be55ceb828dbd73",
                "0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));

    assertThat(addResult).isEqualTo(AllowlistOperationResult.ERROR_DUPLICATED_ENTRY);
  }

  @Test
  public void removeDuplicatedAccountShouldReturnDuplicatedEntryResult() {
    AllowlistOperationResult removeResult =
        controller.removeAccounts(
            Arrays.asList(
                "0xfe3b557e8fb62b89f4916b721be55ceb828dbd73",
                "0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));

    assertThat(removeResult).isEqualTo(AllowlistOperationResult.ERROR_DUPLICATED_ENTRY);
  }

  @Test
  public void removeNullListShouldReturnEmptyEntryResult() {
    AllowlistOperationResult removeResult = controller.removeAccounts(null);

    assertThat(removeResult).isEqualTo(AllowlistOperationResult.ERROR_EMPTY_ENTRY);
  }

  @Test
  public void removeEmptyListShouldReturnEmptyEntryResult() {
    AllowlistOperationResult removeResult = controller.removeAccounts(new ArrayList<>());

    assertThat(removeResult).isEqualTo(AllowlistOperationResult.ERROR_EMPTY_ENTRY);
  }

  @Test
  public void stateShouldRevertIfAllowlistPersistFails()
      throws IOException, AllowlistFileSyncException {
    List<String> newAccount = singletonList("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73");
    List<String> newAccount2 = singletonList("0xfe3b557e8fb62b89f4916b721be55ceb828dbd72");

    assertThat(controller.getAccountAllowlist().size()).isEqualTo(0);

    controller.addAccounts(newAccount);
    assertThat(controller.getAccountAllowlist().size()).isEqualTo(1);

    doThrow(new IOException()).when(allowlistPersistor).updateConfig(any(), any());
    controller.addAccounts(newAccount2);

    assertThat(controller.getAccountAllowlist().size()).isEqualTo(1);
    assertThat(controller.getAccountAllowlist()).isEqualTo(newAccount);

    verify(allowlistPersistor, times(3)).verifyConfigFileMatchesState(any(), any());
    verify(allowlistPersistor, times(2)).updateConfig(any(), any());
    verifyNoMoreInteractions(allowlistPersistor);
  }

  @Test
  public void reloadAccountAllowlistWithValidConfigFileShouldUpdateAllowlist() throws Exception {
    final String expectedAccount = "0x627306090abab3a6e1400e9345bc60c78a8bef57";
    final Path permissionsFile = createPermissionsFileWithAccount(expectedAccount);

    when(permissioningConfig.getAccountPermissioningConfigFilePath())
        .thenReturn(permissionsFile.toAbsolutePath().toString());
    when(permissioningConfig.isAccountAllowlistEnabled()).thenReturn(true);
    when(permissioningConfig.getAccountAllowlist())
        .thenReturn(Arrays.asList("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));
    controller =
        new AccountLocalConfigPermissioningController(
            permissioningConfig, allowlistPersistor, metricsSystem);

    controller.reload();

    assertThat(controller.getAccountAllowlist()).containsExactly(expectedAccount);
  }

  @Test
  public void reloadAccountAllowlistWithErrorReadingConfigFileShouldKeepOldAllowlist() {
    when(permissioningConfig.getAccountPermissioningConfigFilePath()).thenReturn("foo");
    when(permissioningConfig.isAccountAllowlistEnabled()).thenReturn(true);
    when(permissioningConfig.getAccountAllowlist())
        .thenReturn(Arrays.asList("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));
    controller =
        new AccountLocalConfigPermissioningController(
            permissioningConfig, allowlistPersistor, metricsSystem);

    final Throwable thrown = catchThrowable(() -> controller.reload());

    assertThat(thrown)
        .isInstanceOf(RuntimeException.class)
        .hasRootCauseMessage(
            "Unable to read permissioning TOML config file : foo Configuration file does not exist: foo");

    assertThat(controller.getAccountAllowlist())
        .containsExactly("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73");
  }

  @Test
  public void accountThatDoesNotStartWith0xIsNotValid() {
    assertThat(AccountLocalConfigPermissioningController.isValidAccountString("bob")).isFalse();
    assertThat(
            AccountLocalConfigPermissioningController.isValidAccountString(
                "b9b81ee349c3807e46bc71aa2632203c5b462032"))
        .isFalse();
    assertThat(
            AccountLocalConfigPermissioningController.isValidAccountString(
                "0xb9b81ee349c3807e46bc71aa2632203c5b462032"))
        .isTrue();
  }

  @Test
  public void shouldMatchAccountsWithInconsistentCasing() {
    when(permissioningConfig.isAccountAllowlistEnabled()).thenReturn(true);
    when(permissioningConfig.getAccountAllowlist())
        .thenReturn(singletonList("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));
    controller =
        new AccountLocalConfigPermissioningController(
            permissioningConfig, allowlistPersistor, metricsSystem);

    assertThat(controller.contains("0xFE3B557E8Fb62b89F4916B721be55cEb828dBd73")).isTrue();
  }

  @Test
  public void isPermittedShouldCheckIfAccountExistInTheAllowlist() {
    when(permissioningConfig.isAccountAllowlistEnabled()).thenReturn(true);
    when(permissioningConfig.getAccountAllowlist())
        .thenReturn(singletonList("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));
    controller =
        new AccountLocalConfigPermissioningController(
            permissioningConfig, allowlistPersistor, metricsSystem);

    final Transaction transaction = mock(Transaction.class);
    when(transaction.getSender())
        .thenReturn(Address.fromHexString("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73"));

    verifyCountersUntouched();

    boolean permitted = controller.isPermitted(transaction);

    assertThat(permitted).isTrue();

    verifyCountersPermitted();
  }

  @Test
  public void isPermittedShouldReturnFalseIfTransactionDoesNotContainSender() {
    final Transaction transactionWithoutSender = mock(Transaction.class);
    when(transactionWithoutSender.getSender()).thenReturn(null);

    verifyCountersUntouched();

    boolean permitted = controller.isPermitted(transactionWithoutSender);

    assertThat(permitted).isFalse();

    verifyCountersUnpermitted();
  }

  private Path createPermissionsFileWithAccount(final String account) throws IOException {
    final String nodePermissionsFileContent = "accounts-allowlist=[\"" + account + "\"]";
    final Path permissionsFile = Files.createTempFile("account_permissions", "");
    permissionsFile.toFile().deleteOnExit();
    Files.write(permissionsFile, nodePermissionsFileContent.getBytes(StandardCharsets.UTF_8));
    return permissionsFile;
  }

  private void verifyCountersUntouched() {
    verify(checkCounter, times(0)).inc();
    verify(checkPermittedCounter, times(0)).inc();
    verify(checkUnpermittedCounter, times(0)).inc();
  }

  private void verifyCountersPermitted() {
    verify(checkCounter, times(1)).inc();
    verify(checkPermittedCounter, times(1)).inc();
    verify(checkUnpermittedCounter, times(0)).inc();
  }

  private void verifyCountersUnpermitted() {
    verify(checkCounter, times(1)).inc();
    verify(checkPermittedCounter, times(0)).inc();
    verify(checkUnpermittedCounter, times(1)).inc();
  }
}
