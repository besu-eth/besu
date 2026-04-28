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
package org.hyperledger.besu.ethereum.mainnet.block.access.list;

import org.hyperledger.besu.datatypes.Address;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class BlockAccessListBuilderEip7928Test {

  @Test
  void buildSortsAccountsByAddressBytes() {
    final Address addrA = Address.fromHexString("0x1000000000000000000000000000000000000001");
    final Address addrB = Address.fromHexString("0x2000000000000000000000000000000000000002");
    final Address addrC = Address.fromHexString("0x3000000000000000000000000000000000000003");

    final BlockAccessList.BlockAccessListBuilder builder = BlockAccessList.builder();
    builder.getOrCreateAccountBuilder(addrC);
    builder.getOrCreateAccountBuilder(addrA);
    builder.getOrCreateAccountBuilder(addrB);

    final BlockAccessList built = builder.build();

    Assertions.assertThat(built.accountChanges())
        .extracting(BlockAccessList.AccountChanges::address)
        .containsExactly(addrA, addrB, addrC);
  }
}
