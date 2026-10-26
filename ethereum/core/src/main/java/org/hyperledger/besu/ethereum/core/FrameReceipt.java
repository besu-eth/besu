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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.datatypes.Log;

import java.util.List;

/**
 * The receipt of a single EIP-8141 frame.
 *
 * @param status the frame status: 0 failure, 1 success, 2 skipped by a failed atomic batch
 * @param executionGasUsed the execution gas used by the frame, not accounting for refunds
 * @param stateGasUsed the final state gas attributed to the frame
 * @param logs the logs emitted by the frame
 */
public record FrameReceipt(int status, long executionGasUsed, long stateGasUsed, List<Log> logs) {

  /** Frame status: failure. */
  public static final int STATUS_FAILURE = 0;

  /** Frame status: success. */
  public static final int STATUS_SUCCESS = 1;

  /** Frame status: skipped due to a failed atomic batch. */
  public static final int STATUS_SKIPPED = 2;
}
