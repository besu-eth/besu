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
package org.hyperledger.besu.crypto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.bouncycastle.asn1.sec.SECNamedCurves;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.crypto.params.ECDomainParameters;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SECPSignatureTest {
  public static final String CURVE_NAME = "secp256k1";

  public static BigInteger curveOrder;

  @BeforeAll
  public static void setUp() {
    final X9ECParameters params = SECNamedCurves.getByName(CURVE_NAME);
    final ECDomainParameters curve =
        new ECDomainParameters(params.getCurve(), params.getG(), params.getN(), params.getH());
    curveOrder = curve.getN();
  }

  @Test
  public void createSignature() {
    final SECPSignature signature =
        SECPSignature.create(BigInteger.ONE, BigInteger.TEN, (byte) 0, curveOrder);
    assertThat(signature.getR()).isEqualTo(BigInteger.ONE);
    assertThat(signature.getS()).isEqualTo(BigInteger.TEN);
    assertThat(signature.getRecId()).isEqualTo((byte) 0);
  }

  @Test
  public void createSignature_NoR() {
    assertThatThrownBy(() -> SECPSignature.create(null, BigInteger.ZERO, (byte) 27, curveOrder))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void createSignature_NoS() {
    assertThatThrownBy(() -> SECPSignature.create(BigInteger.ZERO, null, (byte) 27, curveOrder))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void encodesRAndSAs32BytesEach() {
    for (final BigInteger value :
        List.of(
            BigInteger.ONE,
            BigInteger.valueOf(0x7f),
            BigInteger.valueOf(0x80),
            BigInteger.TWO.pow(255),
            curveOrder.subtract(BigInteger.ONE))) {
      final SECPSignature signature =
          SECPSignature.create(
              value, value.shiftRight(1).max(BigInteger.ONE), (byte) 1, curveOrder);
      assertThat(signature.encodedBytes())
          .isEqualTo(
              Bytes.concatenate(
                  UInt256.valueOf(signature.getR()),
                  UInt256.valueOf(signature.getS()),
                  Bytes.of(1)));
    }
  }

  @Test
  public void encodingRejectsValuesThatDoNotFit256Bits() {
    final SECPSignature signature =
        new SECPSignature(BigInteger.TWO.pow(256), BigInteger.ONE, (byte) 0);
    assertThatThrownBy(signature::encodedBytes).isInstanceOf(IllegalArgumentException.class);
  }
}
