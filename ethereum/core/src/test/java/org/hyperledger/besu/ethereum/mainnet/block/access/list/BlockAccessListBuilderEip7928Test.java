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
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;

import org.apache.tuweni.units.bigints.UInt256;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class BlockAccessListBuilderEip7928Test {

  private static final Address ADDR_1 =
      Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final Address ADDR_2 =
      Address.fromHexString("0x2000000000000000000000000000000000000002");
  private static final StorageSlotKey SLOT_1 = new StorageSlotKey(UInt256.ONE);

  @Test
  void builderEip7928ItemCountMatchesBuiltList() {
    final BlockAccessList.BlockAccessListBuilder builder = BlockAccessList.builder();
    final BlockAccessList.BlockAccessListBuilder.AccountBuilder ab1 =
        builder.getOrCreateAccountBuilder(ADDR_1);
    ab1.addStorageRead(SLOT_1);
    ab1.addBalanceChange(0, Wei.ONE);
    builder.getOrCreateAccountBuilder(ADDR_2);
    final BlockAccessList built = builder.build();
    Assertions.assertThat(builder.eip7928ItemCount()).isEqualTo(built.eip7928ItemCount());
  }

  /**
   * Tx selection probes the EIP-7928 budget by snapshotting the committed builder ({@code
   * mergeFrom(build())}) then applying the candidate partial; that path must agree with applying
   * partials incrementally on one builder, or the probe count (and accept/reject decision) would be
   * wrong.
   */
  @Test
  void mergeFromReplayMatchesIncrementalApply() {
    final PartialBlockAccessView p0 = partialWithOneAccountAndStorageWrite(0);
    final PartialBlockAccessView p1 = partialWithOneAccountAndStorageWrite(1);
    final BlockAccessList.BlockAccessListBuilder main = BlockAccessList.builder();
    main.apply(p0);
    final BlockAccessList snap = main.build();

    final BlockAccessList.BlockAccessListBuilder viaMerge = BlockAccessList.builder();
    viaMerge.mergeFrom(snap);
    viaMerge.apply(p1);

    final BlockAccessList.BlockAccessListBuilder direct = BlockAccessList.builder();
    direct.apply(p0);
    direct.apply(p1);

    Assertions.assertThat(viaMerge.eip7928ItemCount())
        .isEqualTo(direct.eip7928ItemCount())
        .isEqualTo(4L);
  }

  private static PartialBlockAccessView partialWithOneAccountAndStorageWrite(final int txIndex) {
    final Address addr = Address.fromHexString(String.format("0x%040x", txIndex + 100L));
    final PartialBlockAccessView.PartialBlockAccessViewBuilder b =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(txIndex);
    b.getOrCreateAccountBuilder(addr)
        .addStorageChange(new StorageSlotKey(UInt256.ONE), UInt256.ZERO);
    return b.build();
  }
}
