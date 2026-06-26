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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.json.BesuJsonModule;

import java.math.BigInteger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class BlockOverridesParameterTest {

  private final ObjectMapper mapper =
      new ObjectMapper().registerModule(new Jdk8Module()).registerModule(new BesuJsonModule());

  @Test
  public void shouldDeserializeBlockOverridesParameter() throws Exception {
    final String BLOCK_OVERRIDES_JSON =
        """
        {
          "time": "0x688c5587",
          "number": "0x15fa09a",
          "hash": "0x6a7fd27f1bc68551be2aaade72274f9234e1d473b77451bce237b7cb2c2f3b80",
          "gasLimit": "0x2ad4d9a",
          "feeRecipient": "0x4838b106fce9647bdf1e7877bf73ce8b0bad5f97",
          "baseFeePerGas": "0xecd579a",
          "blobBaseFee": "0x4a817c800",
          "stateRoot": "0xce92856dbb957e101d5393e9c68dbeef1ac0d63853a98f146112d614dd6748f0",
          "difficulty": "1000000000000",
          "mixHash": "0xd9b793d34b9e642d2ee44b5a517c45dc39bae4fe14bfe41e3970645de8775471",
          "parentBeaconBlockRoot": "0x0000000000000000000000000000000000000000000000000000000000000001"
        }
        """;

    final BlockOverridesParameter param =
        mapper.readValue(BLOCK_OVERRIDES_JSON, BlockOverridesParameter.class);

    assertThat(param.getTimestamp()).contains(1754027399L);
    assertThat(param.getBlockNumber()).contains(23044250L);
    assertThat(param.getBlockHash())
        .contains(
            Hash.fromHexString(
                "0x6a7fd27f1bc68551be2aaade72274f9234e1d473b77451bce237b7cb2c2f3b80"));
    assertThat(param.getMixHashOrPrevRandao())
        .contains(
            Bytes32.wrap(
                Hash.fromHexString(
                        "0xd9b793d34b9e642d2ee44b5a517c45dc39bae4fe14bfe41e3970645de8775471")
                    .getBytes()));
    assertThat(param.getGasLimit()).contains(44912026L);
    assertThat(param.getFeeRecipient())
        .contains(Address.fromHexString("0x4838b106fce9647bdf1e7877bf73ce8b0bad5f97"));
    assertThat(param.getBaseFeePerGas()).contains(Wei.fromHexString("0xecd579a"));
    assertThat(param.getBlobBaseFee()).contains(Wei.fromHexString("0x4a817c800"));
    assertThat(param.getStateRoot())
        .contains(
            Hash.fromHexString(
                "0xce92856dbb957e101d5393e9c68dbeef1ac0d63853a98f146112d614dd6748f0"));
    assertThat(param.getDifficulty()).contains(new BigInteger("1000000000000"));
    assertThat(param.getParentBeaconBlockRoot())
        .contains(
            Bytes32.wrap(
                Hash.fromHexString(
                        "0x0000000000000000000000000000000000000000000000000000000000000001")
                    .getBytes()));
  }
}
