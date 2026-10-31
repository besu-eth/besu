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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import java.util.Optional;

import com.google.common.base.Throwables;

/** An attempt of the SNAP2 sync cannot go on, and the sync starts over. */
final class RestartException extends RuntimeException {

  RestartException(final String message) {
    super(message);
  }

  RestartException(final String message, final Throwable cause) {
    super(message, cause);
  }

  /** Returns the restart a failure is, or wraps. */
  static Optional<RestartException> in(final Throwable failure) {
    return Throwables.getCausalChain(failure).stream()
        .filter(RestartException.class::isInstance)
        .map(RestartException.class::cast)
        .findFirst();
  }
}
