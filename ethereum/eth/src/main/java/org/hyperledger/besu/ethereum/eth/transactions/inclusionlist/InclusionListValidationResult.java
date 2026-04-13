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
package org.hyperledger.besu.ethereum.eth.transactions.inclusionlist;

import java.util.Optional;

/** Result of inclusion list validation, containing status and optional error message. */
public class InclusionListValidationResult {

  /** Status codes for inclusion list validation per EIP-7805. */
  public enum Status {
    /** The payload satisfies all inclusion list constraints. */
    VALID,
    /** The inclusion list data itself is malformed or invalid. */
    INVALID,
    /** The payload does not satisfy the inclusion list constraints. */
    UNSATISFIED
  }

  private final Status status;
  private final Optional<String> errorMessage;

  private InclusionListValidationResult(final Status status, final Optional<String> errorMessage) {
    this.status = status;
    this.errorMessage = errorMessage;
  }

  public static InclusionListValidationResult valid() {
    return new InclusionListValidationResult(Status.VALID, Optional.empty());
  }

  public static InclusionListValidationResult invalid(final String errorMessage) {
    return new InclusionListValidationResult(Status.INVALID, Optional.of(errorMessage));
  }

  public static InclusionListValidationResult unsatisfied(final String errorMessage) {
    return new InclusionListValidationResult(Status.UNSATISFIED, Optional.of(errorMessage));
  }

  public Status getStatus() {
    return status;
  }

  public Optional<String> getErrorMessage() {
    return errorMessage;
  }

  public boolean isValid() {
    return status == Status.VALID;
  }
}
