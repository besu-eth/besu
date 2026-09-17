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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;

import org.apache.tuweni.bytes.Bytes32;

/** Shared constants and utilities for parallel block processor integration tests. */
public final class ParallelBlockProcessorTestSupport {

  private ParallelBlockProcessorTestSupport() {}

  public static final String GENESIS_CONFIG =
      "/org/hyperledger/besu/ethereum/mainnet/parallelization/genesis-it.json";

  public static final Address MINING_BENEFICIARY =
      Address.fromHexStringStrict("0xa05b21E5186Ce93d2a226722b85D6e550Ac7D6E3");

  public static final String ACCOUNT_GENESIS_1 = "0x627306090abab3a6e1400e9345bc60c78a8bef57";
  public static final String ACCOUNT_GENESIS_2 = "0x7f2d653f56ea8de6ffa554c7a0cd4e03af79f3eb";
  public static final String ACCOUNT_2 = "0x0000000000000000000000000000000000000002";
  public static final String ACCOUNT_3 = "0x0000000000000000000000000000000000000003";
  public static final String ACCOUNT_4 = "0x0000000000000000000000000000000000000004";
  public static final String ACCOUNT_5 = "0x0000000000000000000000000000000000000005";
  public static final String ACCOUNT_6 = "0x0000000000000000000000000000000000000006";
  public static final String CONTRACT_ADDRESS = "0x00000000000000000000000000000000000fffff";
  public static final String PARALLEL_TEST_CONTRACT = "0x00000000000000000000000000000000000eeeee";

  public static final KeyPair ACCOUNT_GENESIS_1_KEYPAIR =
      generateKeyPair("c87509a1c067bbde78beb793e6fa76530b6382a4c0241e5e4a9ec0a0f44dc0d3");
  public static final KeyPair ACCOUNT_GENESIS_2_KEYPAIR =
      generateKeyPair("fc5141e75bf622179f8eedada7fab3e2e6b3e3da8eb9df4f46d84df22df7430e");
  public static final KeyPair MINING_BENEFICIARY_KEYPAIR =
      generateKeyPair("3a4ff6d22d7502ef2452368165422861c01a0f72f851793b372b87888dc3c453");

  public static KeyPair generateKeyPair(final String privateKeyHex) {
    return SignatureAlgorithmFactory.getInstance()
        .createKeyPair(
            SECPPrivateKey.create(
                Bytes32.fromHexString(privateKeyHex), SignatureAlgorithm.ALGORITHM));
  }
}
