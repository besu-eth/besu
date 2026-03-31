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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie.trielog;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;
import org.hyperledger.besu.ethereum.trie.common.BinaryStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.BinTrieAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.trielogs.TrieLogAccumulator;
import org.hyperledger.besu.plugin.services.trielogs.TrieLogFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * TrieLog factory implementation for BinTrie storage format. Handles serialization and
 * deserialization of trie logs using BinaryStateTrieAccountValue.
 */
public class BinTrieTrieLogFactoryImpl implements TrieLogFactory {

  @Override
  public TrieLogLayer create(final TrieLogAccumulator accumulator, final BlockHeader blockHeader) {
    TrieLogLayer layer = new TrieLogLayer();
    layer.setBlockHash(blockHeader.getBlockHash());
    layer.setBlockNumber(blockHeader.getNumber());

    for (final var updatedAccount : accumulator.getAccountsToUpdate().entrySet()) {
      final var value = updatedAccount.getValue();
      if (value.isUnchanged()) {
        continue;
      }
      final var oldAccountValue = value.getPrior();
      final var newAccountValue = value.getUpdated();
      if (oldAccountValue == null && newAccountValue == null) {
        continue;
      }
      layer.addAccountChange(updatedAccount.getKey(), oldAccountValue, newAccountValue);
    }

    for (final var updatedCode : accumulator.getCodeToUpdate().entrySet()) {
      final var codeValue = updatedCode.getValue();
      if (codeValue.isUnchanged()) {
        continue;
      }
      layer.addCodeChange(
          updatedCode.getKey(),
          codeValue.getPrior(),
          codeValue.getUpdated(),
          blockHeader.getBlockHash());
    }

    for (final var updatesStorage : accumulator.getStorageToUpdate().entrySet()) {
      final Address address = updatesStorage.getKey();
      for (final var slotUpdate : updatesStorage.getValue().entrySet()) {
        var val = slotUpdate.getValue();
        if (val.isUnchanged()) {
          continue;
        }
        if (val.getPrior() == null && val.getUpdated() == null) {
          continue;
        }
        layer.addStorageChange(address, slotUpdate.getKey(), val.getPrior(), val.getUpdated());
      }
    }
    return layer;
  }

  @Override
  public byte[] serialize(final TrieLog layer) {
    final BytesValueRLPOutput rlpLog = new BytesValueRLPOutput();
    writeTo(layer, rlpLog);
    return rlpLog.encoded().toArrayUnsafe();
  }

  public static void writeTo(final TrieLog layer, final RLPOutput output) {
    layer.freeze();

    final Set<Address> addresses = new TreeSet<>();
    addresses.addAll(layer.getAccountChanges().keySet());
    addresses.addAll(layer.getCodeChanges().keySet());
    addresses.addAll(layer.getStorageChanges().keySet());

    output.startList();
    output.writeBytes(layer.getBlockHash().getBytes());
    output.writeInt(DataStorageFormat.BINTRIE.getValue());

    for (final Address address : addresses) {
      output.startList();
      output.writeBytes(address.getBytes());

      final TrieLog.LogTuple<AccountValue> accountChange = layer.getAccountChanges().get(address);
      if (accountChange == null || accountChange.isUnchanged()) {
        output.writeNull();
      } else {
        writeRlp(
            accountChange,
            output,
            (o, account) -> {
              if (account instanceof BinTrieAccount binTrieAccount) {
                new BinaryStateTrieAccountValue(
                        binTrieAccount.getNonce(),
                        binTrieAccount.getBalance(),
                        binTrieAccount.getCodeHash(),
                        binTrieAccount.getCodeSize())
                    .writeTo(o);
              } else {
                account.writeTo(o);
              }
            });
      }

      final TrieLog.LogTuple<Bytes> codeChange = layer.getCodeChanges().get(address);
      if (codeChange == null || codeChange.isUnchanged()) {
        output.writeNull();
      } else {
        writeRlp(codeChange, output, RLPOutput::writeBytes);
      }

      final Map<StorageSlotKey, TrieLog.LogTuple<UInt256>> storageChanges =
          layer.getStorageChanges().get(address);
      if (storageChanges == null) {
        output.writeNull();
      } else {
        output.startList();
        for (final Map.Entry<StorageSlotKey, TrieLog.LogTuple<UInt256>> storageChangeEntry :
            storageChanges.entrySet()) {
          if (!storageChangeEntry.getValue().isUnchanged()) {
            output.startList();
            output.writeBytes(storageChangeEntry.getKey().getSlotHash().getBytes());
            writeInnerRlp(storageChangeEntry.getValue(), output, RLPOutput::writeBytes);
            storageChangeEntry.getKey().getSlotKey().ifPresent(output::writeUInt256Scalar);
            output.endList();
          }
        }
        output.endList();
      }

      output.endList();
    }
    output.endList();
  }

  @Override
  public TrieLogLayer deserialize(final byte[] bytes) {
    return readFrom(new BytesValueRLPInput(Bytes.wrap(bytes), false));
  }

  public static TrieLogLayer readFrom(final RLPInput input) {
    final TrieLogLayer newLayer = new TrieLogLayer();

    input.enterList();
    newLayer.setBlockHash(Hash.wrap(input.readBytes32()));

    if (!input.nextIsList()) {
      input.readInt(); // Skip format value - BinTrie format is implied
    }

    while (!input.isEndOfCurrentList()) {
      input.enterList();
      final Address address = Address.readFrom(input);

      if (input.nextIsNull()) {
        input.skipNext();
      } else {
        input.enterList();
        final BinaryStateTrieAccountValue oldValue =
            nullOrValue(input, BinaryStateTrieAccountValue::readFrom);
        final BinaryStateTrieAccountValue newValue =
            nullOrValue(input, BinaryStateTrieAccountValue::readFrom);
        final boolean isCleared = getOptionalIsCleared(input);
        input.leaveList();
        newLayer
            .getAccountChanges()
            .put(address, new PathBasedValue<>(oldValue, newValue, isCleared));
      }

      if (input.nextIsNull()) {
        input.skipNext();
      } else {
        input.enterList();
        final Bytes oldCode = nullOrValue(input, RLPInput::readBytes);
        final Bytes newCode = nullOrValue(input, RLPInput::readBytes);
        final boolean isCleared = getOptionalIsCleared(input);
        input.leaveList();
        newLayer.getCodeChanges().put(address, new PathBasedValue<>(oldCode, newCode, isCleared));
      }

      if (input.nextIsNull()) {
        input.skipNext();
      } else {
        final Map<StorageSlotKey, PathBasedValue<UInt256>> storageChanges = new TreeMap<>();
        input.enterList();
        while (!input.isEndOfCurrentList()) {
          final int storageElementListSize = input.enterList();
          final Hash slotHash = Hash.wrap(input.readBytes32());
          final UInt256 oldValue =
              nullOrValue(input, rlpInput -> UInt256.fromBytes(rlpInput.readBytes()));
          final UInt256 newValue =
              nullOrValue(input, rlpInput -> UInt256.fromBytes(rlpInput.readBytes()));
          final boolean isCleared = getOptionalIsCleared(input);
          // If list has 5 elements, slotKey is present (slotHash + old + new + isCleared + slotKey)
          final Optional<UInt256> slotKey =
              Optional.of(storageElementListSize)
                  .filter(listSize -> listSize == 5)
                  .map(__ -> input.readUInt256Scalar());

          final StorageSlotKey storageSlotKey = new StorageSlotKey(slotHash, slotKey);
          storageChanges.put(storageSlotKey, new PathBasedValue<>(oldValue, newValue, isCleared));
          input.leaveList();
        }
        input.leaveList();
        newLayer.getStorageChanges().put(address, storageChanges);
      }

      input.leaveListLenient();
    }
    input.leaveListLenient();
    newLayer.freeze();

    return newLayer;
  }

  protected static <T> T nullOrValue(final RLPInput input, final Function<RLPInput, T> reader) {
    if (input.nextIsNull()) {
      input.skipNext();
      return null;
    } else {
      return reader.apply(input);
    }
  }

  protected static boolean getOptionalIsCleared(final RLPInput input) {
    return Optional.of(input.isEndOfCurrentList())
        .filter(isEnd -> !isEnd)
        .map(__ -> nullOrValue(input, RLPInput::readInt))
        .filter(i -> i == 1)
        .isPresent();
  }

  public static <T> void writeRlp(
      final TrieLog.LogTuple<T> value,
      final RLPOutput output,
      final BiConsumer<RLPOutput, T> writer) {
    output.startList();
    writeInnerRlp(value, output, writer);
    output.endList();
  }

  public static <T> void writeInnerRlp(
      final TrieLog.LogTuple<T> value,
      final RLPOutput output,
      final BiConsumer<RLPOutput, T> writer) {
    if (value.getPrior() == null) {
      output.writeNull();
    } else {
      writer.accept(output, value.getPrior());
    }
    if (value.getUpdated() == null) {
      output.writeNull();
    } else {
      writer.accept(output, value.getUpdated());
    }
    if (!value.isLastStepCleared()) {
      output.writeNull();
    } else {
      output.writeInt(1);
    }
  }
}
