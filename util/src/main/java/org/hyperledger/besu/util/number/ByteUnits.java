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
package org.hyperledger.besu.util.number;

/**
 * The Byte units. Follows this spec <a
 * href="https://en.wikipedia.org/wiki/Byte#Multiple-byte_units">Multiple-byte units</a>
 */
public class ByteUnits {
  /** The constant MEBIBYTE. */
  public static final int MEBIBYTE = 1 << 20;

  /** The constant KIBIBYTE. */
  public static final int KIBIBYTE = 1 << 10;

  private ByteUnits() {}
}
