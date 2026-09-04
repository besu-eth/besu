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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.calltrace;

/**
 * Categorizes EVM opcodes for call tracing purposes.
 *
 * <p>This enum provides type-safe classification of opcodes, replacing multiple string comparison
 * methods with a single categorization that can be used throughout the call tracer.
 */
public enum OpcodeCategory {
  /** Call operations: CALL, CALLCODE, DELEGATECALL, STATICCALL */
  CALL,

  /** Contract creation: CREATE, CREATE2 */
  CREATE,

  /** Successful return: RETURN */
  RETURN,

  /** Revert operation: REVERT */
  REVERT,

  /** Halt operation: STOP */
  HALT,

  /** Self-destruct operation: SELFDESTRUCT */
  SELFDESTRUCT,

  /** Any other opcode not relevant for call tracing */
  OTHER;

  // Opcode string constants
  private static final String CALL_OP = "CALL";
  private static final String CALLCODE_OP = "CALLCODE";
  private static final String DELEGATECALL_OP = "DELEGATECALL";
  private static final String STATICCALL_OP = "STATICCALL";
  private static final String CREATE_OP = "CREATE";
  private static final String CREATE2_OP = "CREATE2";
  private static final String RETURN_OP = "RETURN";
  private static final String REVERT_OP = "REVERT";
  private static final String STOP_OP = "STOP";
  private static final String SELFDESTRUCT_OP = "SELFDESTRUCT";

  /**
   * Categorizes an opcode string into its corresponding category.
   *
   * @param opcode the opcode string (e.g., "CALL", "CREATE")
   * @return the category of the opcode, or OTHER if not recognized
   */
  public static OpcodeCategory of(final String opcode) {
    if (opcode == null) {
      return OTHER;
    }

    return switch (opcode) {
      case CALL_OP, CALLCODE_OP, DELEGATECALL_OP, STATICCALL_OP -> CALL;
      case CREATE_OP, CREATE2_OP -> CREATE;
      case RETURN_OP -> RETURN;
      case REVERT_OP -> REVERT;
      case STOP_OP -> HALT;
      case SELFDESTRUCT_OP -> SELFDESTRUCT;
      default -> OTHER;
    };
  }

  /**
   * Checks if the opcode is a call operation.
   *
   * @param opcode the opcode string
   * @return true if it's CALL, CALLCODE, DELEGATECALL, or STATICCALL
   */
  public static boolean isCallOp(final String opcode) {
    return of(opcode) == CALL;
  }
}
