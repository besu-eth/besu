/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import static com.google.common.base.Preconditions.checkArgument;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;

import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.BonsaiTrieLogFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;
import org.hyperledger.besu.util.io.RollingFileReader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;

public class RollingImport {

  public static void main(final String[] arg) throws IOException {
    checkArgument(arg.length == 1, "Single argument is file prefix, like `./layer/besu-layer`");

    final RollingFileReader reader =
        new RollingFileReader((i, c) -> Path.of(String.format(arg[0] + "-%04d.rdat", i)), false);

    final InMemoryKeyValueStorageProvider provider = new InMemoryKeyValueStorageProvider();
    final BonsaiWorldStateProvider archive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(null);
    final BonsaiWorldState bonsaiState =
        new BonsaiWorldState(
            archive,
            new BonsaiWorldStateKeyValueStorage(
                provider, new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new PathBasedCodeCache());
    final SegmentedInMemoryKeyValueStorage worldStateKeyValueStorage =
        (SegmentedInMemoryKeyValueStorage)
            provider.getStorageBySegmentIdentifiers(
                List.of(
                    ACCOUNT_INFO_STATE,
                    CODE_STORAGE,
                    ACCOUNT_STORAGE_STORAGE,
                    TRIE_BRANCH_STORAGE));

    final InMemoryKeyValueStorage trieLogStorage =
        (InMemoryKeyValueStorage)
            provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE);

    int count = 0;
    while (!reader.isDone()) {
      try {
        final byte[] bytes = reader.readBytes();
        if (bytes.length < 1) {
          continue;
        }
        final TrieLogLayer layer =
            BonsaiTrieLogFactory.readFrom(new BytesValueRLPInput(Bytes.wrap(bytes), false));
        final BonsaiWorldStateUpdateAccumulator updater =
            (BonsaiWorldStateUpdateAccumulator) bonsaiState.updater();
        updater.rollForward(layer);
        updater.commit();
        bonsaiState.persist(null);
        if (count % 10000 == 0) {
          System.out.println(". - " + count);
        } else if (count % 100 == 0) {
          System.out.print(".");
          System.out.flush();
        }
      } catch (final Exception e) {
        //        e.printStackTrace(System.out);
        System.out.println(count);
        throw e;
      }
      count++;
    }

    System.out.printf("%nCount %d - now going backwards!%n", count);

    while (count > 0) {
      try {

        count--;
        reader.seek(count);
        final byte[] bytes = reader.readBytes();
        final TrieLogLayer layer =
            BonsaiTrieLogFactory.readFrom(new BytesValueRLPInput(Bytes.wrap(bytes), false));
        final BonsaiWorldStateUpdateAccumulator updater =
            (BonsaiWorldStateUpdateAccumulator) bonsaiState.updater();
        updater.rollBack(layer);
        updater.commit();
        bonsaiState.persist(null);
        if (count % 10000 == 0) {
          System.out.println(". - " + count);
        } else if (count % 100 == 0) {
          System.out.print(".");
          System.out.flush();
        }
      } catch (final Exception e) {
        System.out.println(count);
        throw e;
      }
    }
    System.out.printf("Back to zero!%n");
    worldStateKeyValueStorage.dump(System.out);
    trieLogStorage.dump(System.out);
  }
}
