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
package org.hyperledger.besu.ethereum.eth.sync.snap2.net;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockAccessListsMessageData;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetAccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetBlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetStorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV2;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Encodes snap requests with the response size of the sync, and decodes snap range responses in
 * wire order, without intermediate maps.
 */
public final class SnapMessages {

  private SnapMessages() {}

  /** A GetAccountRange request. */
  public static MessageData getAccountRange(
      final Bytes32 root, final Bytes32 origin, final Bytes32 limit, final long bytes) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(root);
    out.writeBytes(origin);
    out.writeBytes(limit);
    out.writeLongScalar(bytes);
    out.endList();
    return new GetAccountRangeMessage(out.encoded());
  }

  /** A GetStorageRanges request. */
  public static MessageData getStorageRanges(
      final Bytes32 root,
      final List<Bytes32> accounts,
      final Bytes32 origin,
      final Bytes32 limit,
      final long bytes) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(root);
    out.writeList(accounts, (account, list) -> list.writeBytes(account));
    out.writeBytes(origin);
    out.writeBytes(limit);
    out.writeLongScalar(bytes);
    out.endList();
    return new GetStorageRangeMessage(out.encoded());
  }

  /** A GetByteCodes request. */
  public static MessageData getByteCodes(final List<Bytes32> codeHashes, final long bytes) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeList(codeHashes, (hash, list) -> list.writeBytes(hash));
    out.writeLongScalar(bytes);
    out.endList();
    return new GetByteCodesMessage(out.encoded());
  }

  /** A snap/2 GetBlockAccessLists request. */
  public static MessageData getBlockAccessLists(final List<Hash> blockHashes, final long bytes) {
    return new GetBlockAccessListsMessage(
        GetBlockAccessListsMessageData.encodeSnapRequest(blockHashes, BigInteger.valueOf(bytes)));
  }

  /**
   * Decoded account range.
   *
   * @param hashes the account hashes
   * @param accounts the accounts, re-encoded in the full trie format
   * @param proof the boundary proof nodes
   */
  public record AccountRange(List<Bytes32> hashes, List<Bytes> accounts, List<Bytes> proof) {}

  /**
   * Decoded storage ranges, one list per served account.
   *
   * @param slotHashes the slot hashes per account
   * @param slotValues the RLP encoded slot values per account, as stored in the trie
   * @param proof the proof of the last range, empty when every range is complete
   */
  public record StorageRanges(
      List<List<Bytes32>> slotHashes, List<List<Bytes>> slotValues, List<Bytes> proof) {}

  /** Decodes an AccountRange response. */
  public static AccountRange accountRange(final MessageData message) {
    checkCode(message, SnapV2.ACCOUNT_RANGE);
    final RLPInput in = enterResponse(message);
    final List<Bytes32> hashes = new ArrayList<>();
    final List<Bytes> accounts = new ArrayList<>();
    in.enterList();
    while (!in.isEndOfCurrentList()) {
      in.enterList();
      hashes.add(in.readBytes32());
      accounts.add(fullAccount(in));
      in.leaveList();
    }
    in.leaveList();
    final List<Bytes> proof = readByteList(in);
    return new AccountRange(hashes, accounts, proof);
  }

  /** Decodes a StorageRanges response. */
  public static StorageRanges storageRanges(final MessageData message) {
    checkCode(message, SnapV2.STORAGE_RANGE);
    final RLPInput in = enterResponse(message);
    final List<List<Bytes32>> slotHashes = new ArrayList<>();
    final List<List<Bytes>> slotValues = new ArrayList<>();
    in.enterList();
    while (!in.isEndOfCurrentList()) {
      final List<Bytes32> hashes = new ArrayList<>();
      final List<Bytes> values = new ArrayList<>();
      in.enterList();
      while (!in.isEndOfCurrentList()) {
        in.enterList();
        hashes.add(in.readBytes32());
        values.add(in.readBytes());
        in.leaveList();
      }
      in.leaveList();
      slotHashes.add(hashes);
      slotValues.add(values);
    }
    in.leaveList();
    final List<Bytes> proof = readByteList(in);
    return new StorageRanges(slotHashes, slotValues, proof);
  }

  // a slim account in its trie encoding; the nonce and balance are copied as they are, a range
  // proof catching any that is not canonical
  private static Bytes fullAccount(final RLPInput in) {
    in.enterList();
    if (in.nextIsList()) {
      throw new RLPException("invalid account nonce");
    }
    final Bytes nonce = in.readAsRlp().raw();
    if (in.nextIsList()) {
      throw new RLPException("invalid account balance");
    }
    final Bytes balance = in.readAsRlp().raw();
    final Bytes storageRoot = readHashOr(in, Hash.EMPTY_TRIE_HASH.getBytes());
    final Bytes codeHash = readHashOr(in, Hash.EMPTY.getBytes());
    in.leaveList();
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeRaw(nonce);
    out.writeRaw(balance);
    out.writeBytes(storageRoot);
    out.writeBytes(codeHash);
    out.endList();
    return out.encoded();
  }

  // a slim account leaves out the empty storage root and code hash
  private static Bytes readHashOr(final RLPInput in, final Bytes empty) {
    final Bytes value = in.readBytes();
    if (value.isEmpty()) {
      return empty;
    }
    if (value.size() != 32) {
      throw new RLPException("invalid hash size " + value.size());
    }
    return value;
  }

  private static List<Bytes> readByteList(final RLPInput in) {
    final List<Bytes> out = new ArrayList<>();
    if (in.isEndOfCurrentList()) {
      return out;
    }
    in.enterList();
    while (!in.isEndOfCurrentList()) {
      out.add(in.readBytes());
    }
    in.leaveList();
    return out;
  }

  // positions the input after the request id
  private static RLPInput enterResponse(final MessageData message) {
    final RLPInput in = RLP.input(message.getData());
    in.enterList();
    in.skipNext();
    return in;
  }

  private static void checkCode(final MessageData message, final int expected) {
    if (message.getCode() != expected) {
      throw new IllegalArgumentException(
          "unexpected message code " + message.getCode() + ", expected " + expected);
    }
  }
}
