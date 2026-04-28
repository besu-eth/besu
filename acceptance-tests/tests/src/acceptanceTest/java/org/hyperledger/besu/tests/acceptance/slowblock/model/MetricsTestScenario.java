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
package org.hyperledger.besu.tests.acceptance.slowblock.model;

/**
 * Enum representing the different test scenarios for slow block metrics validation. Each scenario
 * targets specific metrics that should be triggered during block execution.
 */
public enum MetricsTestScenario {
  /** Genesis block - no transactions, baseline metrics */
  GENESIS("Genesis Block", "Initial block with no transactions"),

  /** Empty block produced by QBFT consensus - no user transactions */
  EMPTY_BLOCK("Empty Block", "Consensus block with no user transactions"),

  /** Simple ETH transfer between accounts */
  ETH_TRANSFER("ETH Transfer", "Simple value transfer between two accounts"),

  /** Smart contract deployment */
  CONTRACT_DEPLOY("Contract Deploy", "Deployment of a new smart contract"),

  /** Contract call that writes to storage (SSTORE) */
  STORAGE_WRITE("Storage Write (SSTORE)", "Contract call that writes to storage slot"),

  /** Contract call that reads from storage (SLOAD) */
  STORAGE_READ("Storage Read (SLOAD)", "Contract call that reads from storage slot"),

  /** Contract call using CALL opcode to invoke another contract */
  CONTRACT_CALL("Contract Call (CALL)", "Contract invoking another contract via CALL"),

  /** Contract call using EXTCODESIZE to check code size */
  CODE_READ("Code Read (EXTCODESIZE)", "Contract reading external code size"),

  /** EIP-7702 delegation set transaction */
  EIP7702_DELEGATION_SET("EIP-7702 Delegation Set", "Setting code delegation for an account"),

  /** EIP-7702 delegation clear transaction */
  EIP7702_DELEGATION_CLEAR("EIP-7702 Delegation Clear", "Clearing code delegation from an account");

  private final String displayName;
  private final String description;

  MetricsTestScenario(final String displayName, final String description) {
    this.displayName = displayName;
    this.description = description;
  }

  public String getDisplayName() {
    return displayName;
  }

  public String getDescription() {
    return description;
  }

  @Override
  public String toString() {
    return displayName;
  }
}
