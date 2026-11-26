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
 */ package org.hyperledger.besu.ethereum.eth.manager.task;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlockBody;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;

import java.util.Objects;

import org.apache.tuweni.bytes.Bytes32;

public class BodyIdentifier {
  private final Bytes32 transactionsRoot;
  private final Bytes32 ommersHash;
  private final Bytes32 withdrawalsRoot;

  private BodyIdentifier(
      final Bytes32 transactionsRoot, final Bytes32 ommersHash, final Bytes32 withdrawalsRoot) {
    this.transactionsRoot = transactionsRoot;
    this.ommersHash = ommersHash;
    this.withdrawalsRoot = withdrawalsRoot;
  }

  public BodyIdentifier(final BlockHeader header) {
    this(
        header.getTransactionsRoot() == null
            ? null
            : Bytes32.wrap(header.getTransactionsRoot().getBytes()),
        header.getOmmersHash() == null ? null : Bytes32.wrap(header.getOmmersHash().getBytes()),
        header.getWithdrawalsRoot().map(Hash::getBytes).map(Bytes32::wrap).orElse(null));
  }

  public BodyIdentifier(final BlockBody body) {
    this(
        // the body keeps this root, so importing the block doesn't compute it again
        Bytes32.wrap(body.getTransactionsRoot().getBytes()),
        Bytes32.wrap(BodyValidation.ommersHash(body.getOmmers()).getBytes()),
        body.getWithdrawals()
            .map(w -> Bytes32.wrap(BodyValidation.withdrawalsRoot(w).getBytes()))
            .orElse(null));
  }

  public BodyIdentifier(final SyncBlockBody syncBody) {
    this(
        syncBody.getTransactionsRoot() == null
            ? null
            : Bytes32.wrap(syncBody.getTransactionsRoot().getBytes()),
        syncBody.getOmmersHash() == null ? null : Bytes32.wrap(syncBody.getOmmersHash().getBytes()),
        syncBody.getWithdrawalsRoot() == null
            ? null
            : Bytes32.wrap(syncBody.getWithdrawalsRoot().getBytes()));
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BodyIdentifier that = (BodyIdentifier) o;
    return Objects.equals(transactionsRoot, that.transactionsRoot)
        && Objects.equals(ommersHash, that.ommersHash)
        && Objects.equals(withdrawalsRoot, that.withdrawalsRoot);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transactionsRoot, ommersHash, withdrawalsRoot);
  }

  @Override
  public String toString() {
    return "BodyIdentifier{"
        + "transactionsRoot="
        + transactionsRoot
        + ", ommersHash="
        + ommersHash
        + ", withdrawalsRoot="
        + withdrawalsRoot
        + '}';
  }
}
