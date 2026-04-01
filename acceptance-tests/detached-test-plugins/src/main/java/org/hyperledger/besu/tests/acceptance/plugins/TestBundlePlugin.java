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
package org.hyperledger.besu.tests.acceptance.plugins;

import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.SELECTED;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.ProcessableBlockHeader;
import org.hyperledger.besu.plugin.data.TransactionProcessingResult;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.TransactionSelectionService;
import org.hyperledger.besu.plugin.services.txselection.BlockTransactionSelectionService;
import org.hyperledger.besu.plugin.services.txselection.PluginTransactionSelector;
import org.hyperledger.besu.plugin.services.txselection.PluginTransactionSelectorFactory;
import org.hyperledger.besu.plugin.services.txselection.SelectorsStateManager;
import org.hyperledger.besu.plugin.services.txselection.TransactionEvaluationContext;
import org.hyperledger.besu.tests.acceptance.dsl.account.Accounts;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.auto.service.AutoService;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@AutoService(BesuPlugin.class)
public class TestBundlePlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(TestBundlePlugin.class);
  private final List<String> events = new ArrayList<>();
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  private static final KeyPair SENDER_KEYS =
      SIGNATURE_ALGORITHM.createKeyPair(
          SIGNATURE_ALGORITHM.createPrivateKey(
              Bytes32.fromHexString(Accounts.GENESIS_ACCOUNT_THREE_PRIVATE_KEY)));

  private ServiceManager serviceManager;
  private File callbackDir;

  @CommandLine.Option(names = "--plugin-bundle-test-enabled")
  boolean enabled = false;

  @CommandLine.Option(names = "--plugin-bundle-size")
  int bundleSize = 1;

  @CommandLine.Option(names = "--plugin-bundle-failing-nonce")
  int failingNonce = -1;

  @Override
  public void register(final ServiceManager serviceManager) {
    this.serviceManager = serviceManager;
    serviceManager.getService(PicoCLIOptions.class).orElseThrow().addPicoCLIOptions("bundle", this);
  }

  @Override
  public void beforeExternalServices() {
    if (enabled) {
      callbackDir = new File(System.getProperty("besu.plugins.dir", "plugins"));

      serviceManager
          .getService(TransactionSelectionService.class)
          .orElseThrow()
          .registerPluginTransactionSelectorFactory(
              new PluginTransactionSelectorFactory() {
                @Override
                public void selectPendingTransactions(
                    final BlockTransactionSelectionService blockTransactionSelectionService,
                    final ProcessableBlockHeader pendingBlockHeader,
                    final List<? extends org.hyperledger.besu.datatypes.PendingTransaction>
                        candidatePendingTransactions) {

                  if (events.isEmpty()) {
                    boolean bundleFailed = false;

                    for (int i = 0; i < bundleSize; i++) {
                      final var pendingTx =
                          new PendingTransaction.Local(
                              new TransactionTestFixture()
                                  .chainId(Optional.of(BigInteger.valueOf(4)))
                                  .gasLimit(21_000)
                                  .gasPrice(Wei.of(1_000_000_000L))
                                  .to(Optional.of(Address.ZERO))
                                  .nonce(i)
                                  .createTransaction(SENDER_KEYS));

                      final var result =
                          blockTransactionSelectionService.evaluatePendingTransaction(pendingTx);

                      if (!result.selected()) {
                        bundleFailed = true;
                        break;
                      }
                    }
                    if (bundleFailed) {
                      blockTransactionSelectionService.rollback();
                    } else {
                      blockTransactionSelectionService.commit();
                    }
                    writeEvents();
                  }
                }

                @Override
                public PluginTransactionSelector create(
                    final ProcessableBlockHeader pendingBlockHeader,
                    final SelectorsStateManager selectorsStateManager) {
                  return new PluginTransactionSelector() {

                    @Override
                    public TransactionSelectionResult evaluateTransactionPreProcessing(
                        final TransactionEvaluationContext evaluationContext) {
                      final var nonce =
                          evaluationContext.getPendingTransaction().getTransaction().getNonce();
                      LOG.info("Transaction nonce is {}", nonce);
                      if (nonce == failingNonce) {
                        return TransactionSelectionResult.invalid("failing nonce " + nonce);
                      }
                      return SELECTED;
                    }

                    @Override
                    public TransactionSelectionResult evaluateTransactionPostProcessing(
                        final TransactionEvaluationContext evaluationContext,
                        final TransactionProcessingResult processingResult) {
                      return SELECTED;
                    }

                    @Override
                    public void onTransactionSelected(
                        final TransactionEvaluationContext evaluationContext,
                        final TransactionProcessingResult processingResult) {
                      events.add(
                          "SELECTED:%d"
                              .formatted(
                                  evaluationContext
                                      .getPendingTransaction()
                                      .getTransaction()
                                      .getNonce()));
                    }

                    @Override
                    public void onTransactionNotSelected(
                        final TransactionEvaluationContext evaluationContext,
                        final TransactionSelectionResult transactionSelectionResult) {
                      events.add(
                          "%s:%d"
                              .formatted(
                                  transactionSelectionResult,
                                  evaluationContext
                                      .getPendingTransaction()
                                      .getTransaction()
                                      .getNonce()));
                    }
                  };
                }
              });
    }
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  private void writeEvents() {
    try {
      final File callbackFile = new File(callbackDir, "bundle.events");
      if (!callbackFile.getParentFile().exists()) {
        callbackFile.getParentFile().mkdirs();
        callbackFile.getParentFile().deleteOnExit();
      }

      final var content = String.join("\n", events);

      Files.writeString(callbackFile.toPath(), content);
      callbackFile.deleteOnExit();
    } catch (final IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
