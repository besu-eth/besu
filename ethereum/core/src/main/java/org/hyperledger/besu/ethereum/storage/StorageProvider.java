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
package org.hyperledger.besu.ethereum.storage;

import org.hyperledger.besu.ethereum.chain.BlockchainStorage;
import org.hyperledger.besu.ethereum.chain.VariablesStorage;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.WorldStatePreimageStorage;

import java.io.Closeable;
import java.util.List;

public interface StorageProvider extends Closeable {

  VariablesStorage createVariablesStorage();

  BlockchainStorage createBlockchainStorage(
      ProtocolSchedule protocolSchedule,
      VariablesStorage variablesStorage,
      DataStorageConfiguration storageConfiguration);

  WorldStateKeyValueStorage createWorldStateStorage(
      DataStorageConfiguration dataStorageConfiguration);

  WorldStateStorageCoordinator createWorldStateStorageCoordinator(
      DataStorageConfiguration dataStorageConfiguration);

  WorldStatePreimageStorage createWorldStatePreimageStorage();

  KeyValueStorage getStorageBySegmentIdentifier(SegmentIdentifier segment);

  SegmentedKeyValueStorage getStorageBySegmentIdentifiers(List<SegmentIdentifier> segment);
}
