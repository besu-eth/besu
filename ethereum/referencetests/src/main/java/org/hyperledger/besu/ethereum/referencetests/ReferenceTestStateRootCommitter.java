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
package org.hyperledger.besu.ethereum.referencetests;

import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.DefaultStateRootCommitter;

/**
 * State root committer for reference tests that persists address pre-images via {@link
 * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState#hashAndSavePreImage}.
 */
public final class ReferenceTestStateRootCommitter extends DefaultStateRootCommitter {

  public ReferenceTestStateRootCommitter() {
    super((bonsai, address) -> bonsai.hashAndSavePreImage(address.getBytes()));
  }
}
