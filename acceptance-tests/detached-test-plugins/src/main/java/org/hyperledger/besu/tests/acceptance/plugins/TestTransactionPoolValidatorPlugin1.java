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

import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.StartContext;
import org.hyperledger.besu.plugin.services.BlockchainService;
import org.hyperledger.besu.plugin.services.PicoCLIOptions;
import org.hyperledger.besu.plugin.services.TransactionPoolValidatorService;

import java.util.Optional;

import com.google.auto.service.AutoService;
import picocli.CommandLine.Option;

@AutoService(BesuPlugin.class)
public class TestTransactionPoolValidatorPlugin1 implements BesuPlugin {

  @Option(names = "--plugin-txpool-validator1-test-enabled")
  boolean enabled = false;

  // obtained in start(): the validator depends on it, and the pool only consults the validator
  // once every plugin has started
  private volatile BlockchainService blockchainService;

  @Override
  public void defineOptions(final PicoCLIOptions options) {
    options.addPicoCLIOptions("txpool-validator1", this);
  }

  @Override
  public void register(final RegistrationContext context) {
    context
        .getBesuService(TransactionPoolValidatorService.class)
        .registerPluginTransactionValidatorFactory(() -> this::validate);
  }

  private Optional<String> validate(
      final Transaction tx, final boolean isLocal, final boolean hasPriority) {
    final BlockchainService service = blockchainService;
    if (service == null) {
      return Optional.of("Pool validator consulted before start() returned");
    }
    service.getChainHeadHeader();
    return enabled && !tx.getType().equals(TransactionType.FRONTIER)
        ? Optional.of("Only Frontier transactions are allowed here")
        : Optional.empty();
  }

  @Override
  public void start(final StartContext context) {
    blockchainService = context.getBesuService(BlockchainService.class);
  }

  @Override
  public void stop() {}
}
