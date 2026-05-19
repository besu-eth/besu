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
package org.hyperledger.besu.ethereum.eth.manager.exceptions;

public class NoAvailablePeersException extends EthTaskException {

  /**
   * Stackless singleton. This exception is thrown as a flow-control signal in peer-selection retry
   * loops; the stack trace is never inspected. Reusing a single instance avoids allocating a new
   * object and capturing a JVM stack trace on every retry attempt.
   */
  @SuppressWarnings("StaticAssignmentOfThrowable")
  public static final NoAvailablePeersException INSTANCE =
      new NoAvailablePeersException() {
        @Override
        public synchronized Throwable fillInStackTrace() {
          return this;
        }
      };

  public NoAvailablePeersException() {
    super(FailureReason.NO_AVAILABLE_PEERS);
  }
}
