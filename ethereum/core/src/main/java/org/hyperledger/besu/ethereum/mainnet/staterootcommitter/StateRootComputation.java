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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;

import java.util.List;

/** Result of state-root computation: root hash and deferred storage writes. */
public sealed interface StateRootComputation
    permits StateRootComputation.PathBased, StateRootComputation.Forest {

  @FunctionalInterface
  interface UpdaterWrite {
    void applyTo(BonsaiWorldStateKeyValueStorage.Updater updater);
  }

  Hash root();

  void applyTo(BonsaiWorldStateKeyValueStorage.Updater updater);

  static StateRootComputation pathBased(final Hash root, final List<UpdaterWrite> writes) {
    return new PathBased(root, writes);
  }

  static StateRootComputation forest(final Hash root) {
    return new Forest(root);
  }

  record PathBased(Hash root, List<UpdaterWrite> writes) implements StateRootComputation {

    @Override
    public void applyTo(final BonsaiWorldStateKeyValueStorage.Updater updater) {
      writes.forEach(write -> write.applyTo(updater));
    }
  }

  record Forest(Hash root) implements StateRootComputation {

    @Override
    public void applyTo(final BonsaiWorldStateKeyValueStorage.Updater updater) {
      // Forest persistence applies trie updates during root computation.
    }
  }
}
