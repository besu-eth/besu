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
package org.hyperledger.besu.ethereum.core.json;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.LogsBloomFilter;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.Test;

class BesuJsonModuleTest {

  private final ObjectMapper mapper = new ObjectMapper().registerModule(new BesuJsonModule());

  @Test
  void shouldSerializeAndDeserializeCoreHexTypes() throws Exception {
    final Address address = Address.fromHexString("0x0000000000000000000000000000000000000001");
    assertThat(mapper.writeValueAsString(address)).isEqualTo("\"" + address + "\"");
    assertThat(mapper.readValue(mapper.writeValueAsString(address), Address.class))
        .isEqualTo(address);

    final Hash hash = Hash.hash(Bytes32.fromHexStringLenient("0x1234"));
    assertThat(mapper.writeValueAsString(hash)).isEqualTo("\"" + hash + "\"");
    assertThat(mapper.readValue(mapper.writeValueAsString(hash), Hash.class)).isEqualTo(hash);

    final Bytes bytes = Bytes.fromHexString("0x010203");
    assertThat(mapper.writeValueAsString(bytes)).isEqualTo("\"0x010203\"");
    assertThat(mapper.readValue(mapper.writeValueAsString(bytes), Bytes.class)).isEqualTo(bytes);

    final Bytes32 bytes32 = Bytes32.fromHexStringLenient("0x1234");
    assertThat(mapper.writeValueAsString(bytes32)).isEqualTo("\"" + bytes32.toHexString() + "\"");
    assertThat(mapper.readValue(mapper.writeValueAsString(bytes32), Bytes32.class))
        .isEqualTo(bytes32);
  }

  @Test
  void shouldSerializeAndDeserializeCoreQuantityTypes() throws Exception {
    final UInt64 uint64 = UInt64.valueOf(42L);
    assertThat(mapper.writeValueAsString(uint64)).isEqualTo("\"0x2a\"");
    assertThat(mapper.readValue(mapper.writeValueAsString(uint64), UInt64.class)).isEqualTo(uint64);

    final Wei wei = Wei.of(42L);
    assertThat(mapper.writeValueAsString(wei)).isEqualTo("\"0x2a\"");
    assertThat(mapper.readValue(mapper.writeValueAsString(wei), Wei.class)).isEqualTo(wei);

    final BlobGas blobGas = BlobGas.of(42L);
    assertThat(mapper.writeValueAsString(blobGas)).isEqualTo("\"0x2a\"");
    assertThat(mapper.readValue(mapper.writeValueAsString(blobGas), BlobGas.class))
        .isEqualTo(blobGas);
  }

  @Test
  void shouldSerializeAndDeserializeCompositeCoreTypes() throws Exception {
    final LogsBloomFilter logsBloomFilter = LogsBloomFilter.builder().build();
    assertThat(mapper.writeValueAsString(logsBloomFilter)).isEqualTo("\"" + logsBloomFilter + "\"");
    assertThat(mapper.readValue(mapper.writeValueAsString(logsBloomFilter), LogsBloomFilter.class))
        .isEqualTo(logsBloomFilter);

    final Withdrawal withdrawal =
        new Withdrawal(
            UInt64.valueOf(1L),
            UInt64.valueOf(2L),
            Address.fromHexString("0x0000000000000000000000000000000000000003"),
            GWei.of(4L));
    assertThat(mapper.writeValueAsString(withdrawal))
        .isEqualTo(
            "{\"index\":\"0x1\",\"validatorIndex\":\"0x2\",\"address\":\""
                + withdrawal.getAddress()
                + "\",\"amount\":\"0x4\"}");
    assertThat(mapper.readValue(mapper.writeValueAsString(withdrawal), Withdrawal.class))
        .isEqualTo(withdrawal);

    final BlockAccessList blockAccessList = new BlockAccessList(List.of());
    final String blockAccessListJson = mapper.writeValueAsString(blockAccessList);
    assertThat(blockAccessListJson).startsWith("\"0x");
    assertThat(mapper.readValue(blockAccessListJson, BlockAccessList.class))
        .isEqualTo(blockAccessList);
  }
}
