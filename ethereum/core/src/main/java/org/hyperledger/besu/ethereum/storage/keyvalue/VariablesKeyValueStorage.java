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
package org.hyperledger.besu.ethereum.storage.keyvalue;

import static org.hyperledger.besu.ethereum.chain.VariablesStorage.Keys.CHAIN_HEAD_HASH;
import static org.hyperledger.besu.ethereum.chain.VariablesStorage.Keys.FINALIZED_BLOCK_HASH;
import static org.hyperledger.besu.ethereum.chain.VariablesStorage.Keys.FORK_HEADS;
import static org.hyperledger.besu.ethereum.chain.VariablesStorage.Keys.SAFE_BLOCK_HASH;
import static org.hyperledger.besu.ethereum.chain.VariablesStorage.Keys.SEQ_NO_STORE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.VariablesStorage;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import java.util.Collection;
import java.util.Optional;

import com.google.common.collect.Lists;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class VariablesKeyValueStorage implements VariablesStorage {
  final KeyValueStorage variables;

  public VariablesKeyValueStorage(final KeyValueStorage variables) {
    this.variables = variables;
  }

  @Override
  public Optional<Hash> getChainHead() {
    return getVariable(CHAIN_HEAD_HASH).map(this::bytesToHash);
  }

  @Override
  public Collection<Hash> getForkHeads() {
    return getVariable(FORK_HEADS)
        .map(bytes -> RLP.input(bytes).readList(in -> this.bytesToHash(in.readBytes32())))
        .orElse(Lists.newArrayList());
  }

  @Override
  public Optional<Hash> getFinalized() {
    return getVariable(FINALIZED_BLOCK_HASH).map(this::bytesToHash);
  }

  @Override
  public Optional<Hash> getSafeBlock() {
    return getVariable(SAFE_BLOCK_HASH).map(this::bytesToHash);
  }

  @Override
  public Optional<Bytes> getLocalEnrSeqno() {
    return getVariable(SEQ_NO_STORE).map(Bytes::wrap);
  }

  @Override
  public Optional<Hash> getGenesisStateHash() {
    return getVariable(Keys.GENESIS_STATE_HASH).map(this::bytesToHash);
  }

  @Override
  public Optional<Long> getBodiesFrom() {
    return getVariable(Keys.BODIES_FROM).map(Bytes::toLong);
  }

  @Override
  public Optional<Long> getSyncFloor() {
    return getVariable(Keys.SYNC_FLOOR).map(Bytes::toLong);
  }

  @Override
  public Updater updater() {
    return new Updater(variables.startTransaction());
  }

  private Hash bytesToHash(final Bytes bytes) {
    return Hash.wrap(Bytes32.wrap(bytes, 0));
  }

  Optional<Bytes> getVariable(final Keys key) {
    return variables.get(key.toByteArray()).map(Bytes::wrap);
  }

  public static class Updater implements VariablesStorage.Updater {

    private final KeyValueStorageTransaction variablesTransaction;

    Updater(final KeyValueStorageTransaction variablesTransaction) {
      this.variablesTransaction = variablesTransaction;
    }

    @Override
    public void setChainHead(final Hash blockHash) {
      setVariable(CHAIN_HEAD_HASH, blockHash.getBytes());
    }

    @Override
    public void setForkHeads(final Collection<Hash> forkHeadHashes) {
      final Bytes data =
          RLP.encode(
              o -> o.writeList(forkHeadHashes, (val, out) -> out.writeBytes(val.getBytes())));
      setVariable(FORK_HEADS, data);
    }

    @Override
    public void setFinalized(final Hash blockHash) {
      setVariable(FINALIZED_BLOCK_HASH, blockHash.getBytes());
    }

    @Override
    public void setSafeBlock(final Hash blockHash) {
      setVariable(SAFE_BLOCK_HASH, blockHash.getBytes());
    }

    @Override
    public void setLocalEnrSeqno(final Bytes nodeRecord) {
      setVariable(SEQ_NO_STORE, nodeRecord);
    }

    @Override
    public void setGenesisStateHash(final Hash genesisStateHash) {
      setVariable(Keys.GENESIS_STATE_HASH, genesisStateHash.getBytes());
    }

    @Override
    public void setBodiesFrom(final long blockNumber) {
      setVariable(Keys.BODIES_FROM, Bytes.ofUnsignedLong(blockNumber));
    }

    @Override
    public void setSyncFloor(final long blockNumber) {
      setVariable(Keys.SYNC_FLOOR, Bytes.ofUnsignedLong(blockNumber));
    }

    @Override
    public void removeAll() {
      removeVariable(CHAIN_HEAD_HASH);
      removeVariable(FINALIZED_BLOCK_HASH);
      removeVariable(SAFE_BLOCK_HASH);
      removeVariable(FORK_HEADS);
      removeVariable(SEQ_NO_STORE);
    }

    @Override
    public void commit() {
      variablesTransaction.commit();
    }

    @Override
    public void rollback() {
      variablesTransaction.rollback();
    }

    void setVariable(final Keys key, final Bytes value) {
      variablesTransaction.put(key.toByteArray(), value.toArrayUnsafe());
    }

    void removeVariable(final Keys key) {
      variablesTransaction.remove(key.toByteArray());
    }
  }
}
