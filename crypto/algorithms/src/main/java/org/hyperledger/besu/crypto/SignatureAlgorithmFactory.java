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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Signature algorithm factory. */
public class SignatureAlgorithmFactory {

  private static final Logger LOG = LoggerFactory.getLogger(SignatureAlgorithmFactory.class);

  /** The constant DEFAULT_EC_CURVE_NAME. */
  public static final String DEFAULT_EC_CURVE_NAME = "secp256k1";

  /** The constant SECP_256_R1_CURVE_NAME. */
  public static final String SECP_256_R1_CURVE_NAME = "secp256r1";

  private static final SignatureAlgorithm DEFAULT_INSTANCE = new SECP256K1();
  private static SignatureAlgorithm instance = DEFAULT_INSTANCE;

  private SignatureAlgorithmFactory() {}

  /**
   * Sets instance by curve name.
   *
   * @param ecCurve the ec curve name
   * @throws IllegalArgumentException if the curve name is not supported
   */
  public static void switchInstance(final String ecCurve) {
    instance =
        switch (ecCurve) {
          case DEFAULT_EC_CURVE_NAME -> DEFAULT_INSTANCE;
          case SECP_256_R1_CURVE_NAME -> new SECP256R1();
          default ->
              throw new IllegalArgumentException(
                  ecCurve
                      + " is not in the list of valid elliptic curves ["
                      + DEFAULT_EC_CURVE_NAME
                      + ", "
                      + SECP_256_R1_CURVE_NAME
                      + "]");
        };

    if (!DEFAULT_EC_CURVE_NAME.equals(ecCurve)) {
      LOG.info(
          "The signature algorithm uses the elliptic curve {}. The usage of alternative elliptic curves is still experimental.",
          ecCurve);
    }
  }

  /**
   * Gets instance.
   *
   * @return SignatureAlgorithm instance
   */
  public static SignatureAlgorithm getInstance() {
    return instance;
  }

  /** Reset instance to default. */
  @VisibleForTesting
  public static void resetInstance() {
    instance = DEFAULT_INSTANCE;
  }
}
