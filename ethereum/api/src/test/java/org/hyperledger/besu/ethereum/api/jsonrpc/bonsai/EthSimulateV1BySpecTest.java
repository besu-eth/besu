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
package org.hyperledger.besu.ethereum.api.jsonrpc.bonsai;

import static org.hyperledger.besu.ethereum.api.ApiConfiguration.DEFAULT_GAS_CAP;

import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.ImmutableApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.AbstractJsonRpcHttpBySpecTest;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

/**
 * This class is a test runner for the Bonsai test suite. It runs the tests defined in the Bonsai
 * test suite against the Besu implementation of the JSON-RPC API.
 */
public class EthSimulateV1BySpecTest extends AbstractJsonRpcHttpBySpecTest {

  @Override
  protected void doSetup() throws Exception {
    setupBonsaiBlockchain();
    startService();
  }

  @Override
  protected void setupBonsaiBlockchain() {
    blockchainSetupUtil = getBlockchainSetupUtil(DataStorageFormat.BONSAI);
    blockchainSetupUtil.importAllBlocks(HeaderValidationMode.NONE, HeaderValidationMode.NONE);
  }

  @Override
  protected BlockchainSetupUtil getBlockchainSetupUtil(final DataStorageFormat storageFormat) {
    return createBlockchainSetupUtil(
        "eth/simulateV1/chain-data/genesis.json",
        "eth/simulateV1/chain-data/blocks.bin",
        storageFormat);
  }

  @Override
  protected ApiConfiguration createApiConfiguration() {
    return ImmutableApiConfiguration.builder().gasCap(DEFAULT_GAS_CAP).build();
  }

  public static Object[][] specs() {
    return findSpecFiles(new String[] {"eth/simulateV1/specs"});
  }
}
