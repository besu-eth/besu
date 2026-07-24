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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

public class BlockAccessListResultTest {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void shouldSerializeBareArrayWithSpecFieldNames() throws Exception {
    final Address address = Address.fromHexString("0x1234567890123456789012345678901234567890");
    final StorageSlotKey slot = new StorageSlotKey(UInt256.ONE);
    final AccountChanges accountChanges =
        new AccountChanges(
            address,
            List.of(new SlotChanges(slot, List.of(new StorageChange(1, UInt256.valueOf(100))))),
            Collections.emptyList(),
            List.of(new BalanceChange(1, Wei.of(1000))),
            List.of(new NonceChange(1, 5L)),
            List.of(new CodeChange(1, Bytes.fromHexString("0x60806040"))));

    final String json =
        objectMapper.writeValueAsString(
            BlockAccessListResult.fromBlockAccessList(
                new BlockAccessList(List.of(accountChanges))));

    assertThat(json).startsWith("[");
    assertThat(json)
        .contains("\"key\":\"0x0000000000000000000000000000000000000000000000000000000000000001\"");
    assertThat(json).contains("\"index\":\"0x1\"");
    assertThat(json)
        .contains(
            "\"value\":\"0x0000000000000000000000000000000000000000000000000000000000000064\"");
    assertThat(json).contains("\"value\":\"0x3e8\"");
    assertThat(json).contains("\"value\":\"0x5\"");
    assertThat(json).contains("\"code\":\"0x60806040\"");
    assertThat(json).contains("\"storageReads\":[]");
    assertThat(json).doesNotContain("accountChanges");
    assertThat(json).doesNotContain("txIndex");
    assertThat(json).doesNotContain("newValue");
    assertThat(json).doesNotContain("postBalance");
    assertThat(json).doesNotContain("newNonce");
    assertThat(json).doesNotContain("newCode");
  }
}
