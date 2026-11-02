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
package org.hyperledger.besu.ethereum.chain;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.hyperledger.besu.datatypes.Hash;

import java.util.Collection;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public interface VariablesStorage {
  enum Keys {
    CHAIN_HEAD_HASH("chainHeadHash"),
    FORK_HEADS("forkHeads"),
    FINALIZED_BLOCK_HASH("finalizedBlockHash"),
    SAFE_BLOCK_HASH("safeBlockHash"),
    SEQ_NO_STORE("local-enr-seqno"),
    GENESIS_STATE_HASH("genesisStateHash"),
    BODIES_FROM("bodiesFrom"),
    SYNC_FLOOR("syncFloor");

    private final String key;
    private final byte[] byteArray;
    private final Bytes bytes;

    Keys(final String key) {
      this.key = key;
      this.byteArray = key.getBytes(UTF_8);
      this.bytes = Bytes.wrap(byteArray);
    }

    public byte[] toByteArray() {
      return byteArray;
    }

    public Bytes getBytes() {
      return bytes;
    }

    @Override
    public String toString() {
      return key;
    }
  }

  Optional<Hash> getChainHead();

  Collection<Hash> getForkHeads();

  Optional<Hash> getFinalized();

  Optional<Hash> getSafeBlock();

  Optional<Bytes> getLocalEnrSeqno();

  Optional<Hash> getGenesisStateHash();

  /** Returns the block from which a sync stored every body and receipts up to the chain head. */
  Optional<Long> getBodiesFrom();

  /** Returns the floor of the last sync. */
  Optional<Long> getSyncFloor();

  Updater updater();

  interface Updater {

    void setChainHead(Hash blockHash);

    void setForkHeads(Collection<Hash> forkHeadHashes);

    void setFinalized(Hash blockHash);

    void setSafeBlock(Hash blockHash);

    void setLocalEnrSeqno(Bytes nodeRecord);

    void setGenesisStateHash(Hash genesisStateHash);

    void setBodiesFrom(long blockNumber);

    void setSyncFloor(long blockNumber);

    void removeAll();

    void commit();

    void rollback();
  }
}
