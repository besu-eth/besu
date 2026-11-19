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
package org.hyperledger.besu.ethereum.worldstate;

import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.MissingCodeException;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;

import com.google.common.base.Throwables;

/** Classification of missing local data without mistaking I/O or shutdown for missing state. */
public final class WorldStateFailure {
  private WorldStateFailure() {}

  /** Returns whether a causal chain describes missing trie/code data eligible for repair. */
  public static boolean isMissingState(final Throwable failure) {
    boolean missing = false;
    for (final Throwable cause : Throwables.getCausalChain(failure)) {
      if ((cause instanceof StorageException && !(cause instanceof MissingCodeException))
          || cause instanceof IOException
          || cause instanceof CancellationException
          || cause instanceof InterruptedException
          || cause instanceof RejectedExecutionException) {
        return false;
      }
      missing |= cause instanceof MerkleTrieException || cause instanceof MissingCodeException;
    }
    return missing;
  }
}
