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
package org.hyperledger.besu.ethereum.referencetests;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotRead;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AccountChangesJson {
  private final String address;
  private final List<SlotChangesJson> storageChanges;
  private final List<String> storageReads;
  private final List<BalanceChangeJson> balanceChanges;
  private final List<NonceChangeJson> nonceChanges;
  private final List<CodeChangeJson> codeChanges;

  @JsonCreator
  public AccountChangesJson(
      @JsonProperty("address") final String address,
      @JsonProperty("storageChanges") final List<SlotChangesJson> storageChanges,
      @JsonProperty("storageReads") final List<String> storageReads,
      @JsonProperty("balanceChanges") final List<BalanceChangeJson> balanceChanges,
      @JsonProperty("nonceChanges") final List<NonceChangeJson> nonceChanges,
      @JsonProperty("codeChanges") final List<CodeChangeJson> codeChanges) {
    this.address = address;
    this.storageChanges = storageChanges != null ? storageChanges : Collections.emptyList();
    this.storageReads = storageReads != null ? storageReads : Collections.emptyList();
    this.balanceChanges = balanceChanges != null ? balanceChanges : Collections.emptyList();
    this.nonceChanges = nonceChanges != null ? nonceChanges : Collections.emptyList();
    this.codeChanges = codeChanges != null ? codeChanges : Collections.emptyList();
  }

  public AccountChanges toAccountChanges() {
    return new AccountChanges(
        Address.fromHexString(address),
        storageChanges.stream().map(SlotChangesJson::toSlotChanges).toList(),
        storageReads.stream()
            .map(s -> new SlotRead(new StorageSlotKey(UInt256.fromHexString(s))))
            .toList(),
        balanceChanges.stream().map(BalanceChangeJson::toBalanceChange).toList(),
        nonceChanges.stream().map(NonceChangeJson::toNonceChange).toList(),
        codeChanges.stream().map(CodeChangeJson::toCodeChange).toList());
  }

  public static BlockAccessList toBlockAccessList(final List<AccountChangesJson> accountChanges) {
    if (accountChanges == null) {
      return null;
    }
    return new BlockAccessList(
        accountChanges.stream().map(AccountChangesJson::toAccountChanges).toList());
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SlotChangesJson {
    private final String slot;
    private final List<StorageChangeJson> slotChanges;

    @JsonCreator
    public SlotChangesJson(
        @JsonProperty("slot") final String slot,
        @JsonProperty("slotChanges") final List<StorageChangeJson> slotChanges) {
      this.slot = slot;
      this.slotChanges = slotChanges != null ? slotChanges : Collections.emptyList();
    }

    public SlotChanges toSlotChanges() {
      return new SlotChanges(
          new StorageSlotKey(UInt256.fromHexString(slot)),
          slotChanges.stream().map(StorageChangeJson::toStorageChange).toList());
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class StorageChangeJson {
    private final String blockAccessIndex;
    private final String postValue;

    @JsonCreator
    public StorageChangeJson(
        @JsonProperty("blockAccessIndex") final String blockAccessIndex,
        @JsonProperty("postValue") final String postValue) {
      this.blockAccessIndex = blockAccessIndex;
      this.postValue = postValue;
    }

    public StorageChange toStorageChange() {
      return new StorageChange(
          decodeIndex(blockAccessIndex),
          postValue != null ? UInt256.fromHexString(postValue) : UInt256.ZERO);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class BalanceChangeJson {
    private final String blockAccessIndex;
    private final String postBalance;

    @JsonCreator
    public BalanceChangeJson(
        @JsonProperty("blockAccessIndex") final String blockAccessIndex,
        @JsonProperty("postBalance") final String postBalance) {
      this.blockAccessIndex = blockAccessIndex;
      this.postBalance = postBalance;
    }

    public BalanceChange toBalanceChange() {
      return new BalanceChange(
          decodeIndex(blockAccessIndex),
          postBalance != null ? Wei.fromHexString(postBalance) : Wei.ZERO);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class NonceChangeJson {
    private final String blockAccessIndex;
    private final String postNonce;

    @JsonCreator
    public NonceChangeJson(
        @JsonProperty("blockAccessIndex") final String blockAccessIndex,
        @JsonProperty("postNonce") final String postNonce) {
      this.blockAccessIndex = blockAccessIndex;
      this.postNonce = postNonce;
    }

    public NonceChange toNonceChange() {
      return new NonceChange(
          decodeIndex(blockAccessIndex), postNonce != null ? Long.decode(postNonce) : 0L);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class CodeChangeJson {
    private final String blockAccessIndex;
    private final String newCode;

    @JsonCreator
    public CodeChangeJson(
        @JsonProperty("blockAccessIndex") final String blockAccessIndex,
        @JsonProperty("newCode") final String newCode) {
      this.blockAccessIndex = blockAccessIndex;
      this.newCode = newCode;
    }

    public CodeChange toCodeChange() {
      return new CodeChange(
          decodeIndex(blockAccessIndex),
          newCode != null ? Bytes.fromHexString(newCode) : Bytes.EMPTY);
    }
  }

  private static long decodeIndex(final String index) {
    if (index == null) {
      return 0L;
    }
    final String s = index.toLowerCase(Locale.ROOT);
    if (s.startsWith("0x")) {
      return Long.parseUnsignedLong(s.substring(2), 16);
    }
    return Long.parseUnsignedLong(index, 10);
  }
}
