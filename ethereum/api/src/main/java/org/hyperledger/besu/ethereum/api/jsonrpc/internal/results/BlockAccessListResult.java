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

import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.tuweni.units.bigints.UInt256;

public final class BlockAccessListResult {

  private BlockAccessListResult() {}

  public static List<AccountChangesResult> fromBlockAccessList(final BlockAccessList list) {
    return list.accountChanges().stream().map(AccountChangesResult::new).toList();
  }

  @JsonPropertyOrder({
    "address",
    "storageChanges",
    "storageReads",
    "balanceChanges",
    "nonceChanges",
    "codeChanges"
  })
  public static class AccountChangesResult {
    public final String address;
    public final List<SlotChangeResult> storageChanges;
    public final List<String> storageReads;
    public final List<BalanceChangeResult> balanceChanges;
    public final List<NonceChangeResult> nonceChanges;
    public final List<CodeChangeResult> codeChanges;

    public AccountChangesResult(final AccountChanges changes) {
      this.address = changes.address().toString();
      this.storageChanges = changes.storageChanges().stream().map(SlotChangeResult::new).toList();
      this.storageReads =
          changes.storageReads().stream()
              .map(sr -> sr.slot().getSlotKey().map(UInt256::toHexString).orElse(""))
              .toList();
      this.balanceChanges =
          changes.balanceChanges().stream().map(BalanceChangeResult::new).toList();
      this.nonceChanges = changes.nonceChanges().stream().map(NonceChangeResult::new).toList();
      this.codeChanges = changes.codeChanges().stream().map(CodeChangeResult::new).toList();
    }
  }

  @JsonPropertyOrder({"key", "changes"})
  public static class SlotChangeResult {
    @JsonProperty("key")
    public final String key;

    public final List<StorageChangeResult> changes;

    public SlotChangeResult(final SlotChanges changes) {
      this.key = changes.slot().getSlotKey().map(UInt256::toHexString).orElse("null");
      this.changes = changes.changes().stream().map(StorageChangeResult::new).toList();
    }
  }

  public static class StorageChangeResult {
    public final String index;
    public final String value;

    public StorageChangeResult(final StorageChange change) {
      this.index = Quantity.create(change.txIndex());
      this.value = change.newValue().toHexString();
    }
  }

  public static class BalanceChangeResult {
    public final String index;
    public final String value;

    public BalanceChangeResult(final BalanceChange change) {
      this.index = Quantity.create(change.txIndex());
      this.value = change.postBalance().toShortHexString();
    }
  }

  public static class NonceChangeResult {
    public final String index;
    public final String value;

    public NonceChangeResult(final NonceChange change) {
      this.index = Quantity.create(change.txIndex());
      this.value = Quantity.create(change.newNonce());
    }
  }

  @JsonPropertyOrder({"index", "code"})
  public static class CodeChangeResult {
    public final String index;

    @JsonProperty("code")
    public final String code;

    public CodeChangeResult(final CodeChange change) {
      this.index = Quantity.create(change.txIndex());
      this.code = change.newCode().toHexString();
    }
  }
}
