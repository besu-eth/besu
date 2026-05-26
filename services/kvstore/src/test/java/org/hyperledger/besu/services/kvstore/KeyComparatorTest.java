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
package org.hyperledger.besu.services.kvstore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class KeyComparatorTest {

  @Test
  public void testEmptyVs01() {
    Bytes key1 = Bytes.EMPTY;
    Bytes key2 = Bytes.fromHexString("0x01");
    int result = KeyComparator.compareKeyLeftToRight(key1, key2);
    assertEquals(-1, result, "Empty key should be considered smaller than 0x01");
  }

  @Test
  public void test01Vs02() {
    Bytes key1 = Bytes.fromHexString("0x01");
    Bytes key2 = Bytes.fromHexString("0x02");
    int result = KeyComparator.compareKeyLeftToRight(key1, key2);
    assertEquals(-1, result, "0x01 should be considered smaller than 0x02");
  }

  @Test
  public void test01Vs0100() {
    Bytes key1 = Bytes.fromHexString("0x01");
    Bytes key2 = Bytes.fromHexString("0x0100");
    int result = KeyComparator.compareKeyLeftToRight(key1, key2);
    assertEquals(-1, result, "0x01 should be considered smaller than 0x0100");
  }

  @Test
  public void test01Vs0201() {
    Bytes key1 = Bytes.fromHexString("0x01");
    Bytes key2 = Bytes.fromHexString("0x0201");
    int result = KeyComparator.compareKeyLeftToRight(key1, key2);
    assertEquals(-1, result, "0x01 should be considered smaller than 0x0201");
  }

  @Test
  public void test0101Vs02() {
    Bytes key1 = Bytes.fromHexString("0x0101");
    Bytes key2 = Bytes.fromHexString("0x02");
    int result = KeyComparator.compareKeyLeftToRight(key1, key2);
    assertEquals(-1, result, "0x0101 should be considered smaller than 0x02");
  }

  @Test
  public void test0101Vs0211() {
    Bytes key1 = Bytes.fromHexString("0x0000");
    Bytes key2 = Bytes.fromHexString("0x000000");
    int result = KeyComparator.compareKeyLeftToRight(key1, key2);
    assertEquals(-1, result, "0x0101 should be considered smaller than 0x02");
  }
}
