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
package org.hyperledger.besu.ethereum.transaction;

import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Optional;
import java.util.function.BiFunction;

/**
 * This class is passed to the transaction simulator and allows executing code before the copy of
 * the worldstate is closed. When processing a transaction with the simulator, a copy of the
 * worldstate is created, and once the simulation process is complete, the copy is closed. In some
 * cases the simulator fills a Tracer that requires updates from the worldstate. If we want to
 * generate a trace after the execution, it would fail. This interface allows us to execute our code
 * before the worldstate is closed, ensuring that we can generate our objects without any issues.
 * Then, the simulator will properly close the copy of the worldstate.
 */
public interface PreCloseStateHandler<U>
    extends BiFunction<
        MutableWorldState, Optional<TransactionSimulatorResult>, Optional<? extends U>> {

  /**
   * This method executes code before the worldstate is closed. It takes two parameters:
   * mutableWorldState, which represents the worldstate before it is closed, and
   * transactionSimulatorResult, which holds the result of the transaction simulation. The method
   * returns an Optional object containing the created object. The specific type of the object can
   * vary based on the implementation and is determined by the caller's needs. The purpose of this
   * method is to provide a hook for executing custom code or generating objects before the
   * worldstate is closed, allowing for any necessary processing or operations to be performed.
   *
   * @param mutableWorldState The worldstate before it is closed.
   * @param transactionSimulatorResult The result of the transaction simulation.
   * @return The object that has been created.
   */
  @Override
  Optional<U> apply(
      final MutableWorldState mutableWorldState,
      final Optional<TransactionSimulatorResult> transactionSimulatorResult);
}
