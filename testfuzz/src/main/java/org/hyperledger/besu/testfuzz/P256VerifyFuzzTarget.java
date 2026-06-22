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
package org.hyperledger.besu.testfuzz;

import static org.mockito.Mockito.mock;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECP256R1;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.OsakaGasCalculator;
import org.hyperledger.besu.evm.precompile.P256VerifyPrecompiledContract;
import org.hyperledger.besu.evm.precompile.PrecompiledContract;
import org.hyperledger.besu.testfuzz.javafuzz.FuzzTarget;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Arrays;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;
import org.bouncycastle.asn1.sec.SECNamedCurves;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.math.ec.ECPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * P256 signature verification fuzz target.
 *
 * <p>This fuzzer comprehensively tests the P256VerifyPrecompiledContract by:
 *
 * <ul>
 *   <li>Generating signatures using both Java and Native implementations
 *   <li>Verifying each signature using both Java and Native implementations
 *   <li>Testing edge cases and boundary conditions
 *   <li>Applying various mutation strategies to find discrepancies
 * </ul>
 *
 * <p>The test matrix ensures all combinations are tested:
 *
 * <ul>
 *   <li>Java-generated signature → Java verification
 *   <li>Java-generated signature → Native verification
 *   <li>Native-generated signature → Java verification
 *   <li>Native-generated signature → Native verification
 * </ul>
 *
 * <p><b>THREAD SAFETY:</b> This class is designed for single-threaded use only. It uses static
 * methods to toggle between Java and BoringSSL implementations, which would not be safe in a
 * multi-threaded context. The javafuzz framework executes fuzz() calls sequentially, making this
 * safe for fuzzing.
 */
@SuppressWarnings({
  "DoNotCreateSecureRandomDirectly",
  "MethodInputParametersMustBeFinal",
  "UnusedVariable",
  "NarrowingCompoundAssignment"
})
public class P256VerifyFuzzTarget implements FuzzTarget {

  private static final Logger LOG = LoggerFactory.getLogger(P256VerifyFuzzTarget.class);

  // SECP256R1 curve parameters
  private static final X9ECParameters R1_PARAMS = SECNamedCurves.getByName("secp256r1");
  private static final BigInteger N = R1_PARAMS.getN(); // Order of the curve
  private static final BigInteger P =
      R1_PARAMS.getCurve().getField().getCharacteristic(); // Field prime
  private static final ECPoint G = R1_PARAMS.getG(); // Generator point

  // Expected verification results
  private static final Bytes32 VALID_SIGNATURE_RESULT = Bytes32.leftPad(Bytes.of(1), (byte) 0);

  // Core components
  private final P256VerifyPrecompiledContract verificationContractNative;
  private final P256VerifyPrecompiledContract verificationContractJava;
  private final MessageFrame messageFrame;
  private final SignatureAlgorithm javaSignatureAlgorithm;
  private final SignatureAlgorithm nativeSignatureAlgorithm;
  private final SecureRandom random;

  // Mutation strategies for comprehensive testing
  private enum MutationStrategy {
    NO_MUTATION, // Use valid signature as-is
    BIT_FLIP, // Flip random bits in the signature
    BOUNDARY_VALUES, // Test boundary conditions (0, n-1, n, p-1, p)
    ARITHMETIC_MUTATION, // Apply arithmetic operations to signature components
    CURVE_ATTACKS, // Test curve-specific edge cases
    SIGNATURE_MALLEABILITY, // Test signature malleability (s vs n-s)
    POINT_MANIPULATION, // Manipulate public key coordinates
    BOUNCYCASTLE_BYPASS // Test BouncyCastle validation bypass scenarios
  }

  /** Initializes the fuzz target with both Java and Native signature implementations. */
  public P256VerifyFuzzTarget() {
    this.messageFrame = mock(MessageFrame.class);

    // Create separate instances for Java and Native signing
    this.javaSignatureAlgorithm = createJavaSignatureAlgorithm();
    this.nativeSignatureAlgorithm = createNativeSignatureAlgorithm();

    // Create separate precompile instances for Java and Native verification
    this.verificationContractNative =
        new P256VerifyPrecompiledContract(new OsakaGasCalculator(), this.nativeSignatureAlgorithm);
    this.verificationContractJava =
        new P256VerifyPrecompiledContract(new OsakaGasCalculator(), this.javaSignatureAlgorithm);

    this.random = new SecureRandom();
  }

  /**
   * Creates a SECP256R1 instance configured to use Java implementation for signature generation.
   *
   * <p>Note: This controls signature GENERATION, not verification. Verification implementation
   * (Java vs BoringSSL) is controlled by P256VerifyPrecompiledContract static methods.
   */
  private SignatureAlgorithm createJavaSignatureAlgorithm() {
    SECP256R1 algo = new SECP256R1();
    algo.disableNative(); // Force Java implementation for signature generation
    return algo;
  }

  /**
   * Creates a SECP256R1 instance for native signature generation.
   *
   * <p>Note: This instance is only used for signature GENERATION during testing. The verification
   * implementation (Java vs BoringSSL) is controlled separately via
   * P256VerifyPrecompiledContract.disableNativeBoringSSL() and
   * P256VerifyPrecompiledContract.maybeEnableNativeBoringSSL() static methods.
   *
   * <p>The SignatureAlgorithm has its own native implementation (different from BoringSSL), but it
   * is not typically used in production. This fuzzer focuses on comparing BoringSSL vs Java for
   * verification.
   */
  private SignatureAlgorithm createNativeSignatureAlgorithm() {
    // Return basic SECP256R1 instance - verification implementation is controlled separately
    return new SECP256R1();
  }

  @Override
  public void fuzz(byte[] data) {
    if (data.length < 2) {
      return; // Need at least strategy selection bytes
    }

    try {
      // Select mutation strategy based on fuzzer input
      MutationStrategy strategy = selectMutationStrategy(data[0]);

      // Execute the fuzzing test based on the strategy
      if (requiresSignatureGeneration(strategy)) {
        // For strategies that need valid signatures, generate and test all combinations
        testSignatureGenerationAndVerification(data, strategy);
      } else {
        // For strategies that create synthetic inputs, test verification only
        testSyntheticInputVerification(data, strategy);
      }

    } catch (AssertionError e) {
      // Re-throw assertion errors to trigger fuzzer crash detection
      throw e;
    } catch (Exception e) {
      // Log other exceptions for debugging but don't crash the fuzzer
      LOG.debug("Exception during fuzzing: {}", e.getMessage());
    }
  }

  /** Selects a mutation strategy based on the fuzzer input. */
  private MutationStrategy selectMutationStrategy(byte strategyByte) {
    MutationStrategy[] strategies = MutationStrategy.values();
    return strategies[(strategyByte & 0xFF) % strategies.length];
  }

  /** Determines if a strategy requires generating valid signatures. */
  private boolean requiresSignatureGeneration(MutationStrategy strategy) {
    return switch (strategy) {
      case NO_MUTATION, BIT_FLIP, SIGNATURE_MALLEABILITY, ARITHMETIC_MUTATION -> true;
      default -> false;
    };
  }

  /**
   * Tests signature generation with both implementations and verification with both
   * implementations. This creates the proper test matrix: - Java-generated → Java-verified -
   * Java-generated → Native-verified - Native-generated → Java-verified - Native-generated →
   * Native-verified
   */
  private void testSignatureGenerationAndVerification(byte[] data, MutationStrategy strategy) {
    // Generate signatures with both implementations
    SignatureTestCase javaGeneratedCase =
        generateSignatureTestCase(javaSignatureAlgorithm, "Java", data, strategy);
    SignatureTestCase nativeGeneratedCase =
        generateSignatureTestCase(nativeSignatureAlgorithm, "Native", data, strategy);

    if (javaGeneratedCase == null || nativeGeneratedCase == null) {
      return; // Skip if generation failed
    }

    // Test all verification combinations
    verifySignatureWithBothImplementations(javaGeneratedCase);
    verifySignatureWithBothImplementations(nativeGeneratedCase);

    // Cross-validate: both should produce same results for valid signatures
    if (strategy == MutationStrategy.NO_MUTATION) {
      crossValidateSignatures(javaGeneratedCase, nativeGeneratedCase);
    }
  }

  /**
   * Tests synthetic inputs (not based on real signatures) with both verification implementations.
   */
  private void testSyntheticInputVerification(byte[] data, MutationStrategy strategy) {
    byte[] syntheticInput = generateSyntheticInput(data, strategy);

    if (syntheticInput.length != 160) {
      return; // Skip invalid inputs
    }

    SignatureTestCase syntheticCase =
        new SignatureTestCase(Bytes.wrap(syntheticInput), "Synthetic", strategy);

    verifySignatureWithBothImplementations(syntheticCase);
  }

  /** Represents a test case containing signature data and metadata. */
  private static class SignatureTestCase {
    final Bytes signatureData;
    final String generationMethod;
    final MutationStrategy strategy;

    SignatureTestCase(Bytes signatureData, String generationMethod, MutationStrategy strategy) {
      this.signatureData = signatureData;
      this.generationMethod = generationMethod;
      this.strategy = strategy;
    }
  }

  /** Generates a signature test case using the specified signature algorithm. */
  private SignatureTestCase generateSignatureTestCase(
      SignatureAlgorithm sigAlgo,
      String algorithmName,
      byte[] fuzzData,
      MutationStrategy strategy) {

    try {
      // Generate a valid signature
      byte[] signatureData = generateValidSignature(sigAlgo);

      // Apply mutation based on strategy
      signatureData = applyMutation(signatureData, fuzzData, strategy);

      return new SignatureTestCase(Bytes.wrap(signatureData), algorithmName, strategy);

    } catch (Exception e) {
      LOG.debug("Failed to generate signature with {}: {}", algorithmName, e.getMessage());
      return null;
    }
  }

  /** Generates a valid P256 signature using the specified algorithm. */
  private byte[] generateValidSignature(SignatureAlgorithm sigAlgo) {
    // Generate a key pair
    KeyPair keyPair = sigAlgo.generateKeyPair();

    // Generate random message hash
    Bytes32 messageHash = generateRandomBytes32();

    // Sign the message
    SECPSignature signature = sigAlgo.sign(messageHash, keyPair);

    // Pack into 160-byte format: hash(32) + r(32) + s(32) + qx(32) + qy(32)
    return packSignatureData(messageHash, signature, keyPair);
  }

  /** Packs signature components into the 160-byte input format. */
  private byte[] packSignatureData(Bytes32 messageHash, SECPSignature signature, KeyPair keyPair) {
    MutableBytes packed = MutableBytes.create(160);

    // Message hash (32 bytes)
    packed.set(0, messageHash);

    // Signature r value (32 bytes)
    writeBigIntegerAs32Bytes(packed, 32, signature.getR());

    // Signature s value (32 bytes)
    writeBigIntegerAs32Bytes(packed, 64, signature.getS());

    // Public key coordinates (64 bytes total: qx + qy)
    Bytes publicKeyBytes = keyPair.getPublicKey().getEncodedBytes();
    if (publicKeyBytes.size() == 64) {
      packed.set(96, publicKeyBytes.slice(0, 32)); // qx
      packed.set(128, publicKeyBytes.slice(32, 32)); // qy
    } else {
      LOG.warn("Unexpected public key format, size: {}", publicKeyBytes.size());
      // Fill with zeros if format is unexpected
      packed.set(96, Bytes32.ZERO);
      packed.set(128, Bytes32.ZERO);
    }

    return packed.toArray();
  }

  /** Applies mutation to signature data based on the strategy. */
  private byte[] applyMutation(byte[] signatureData, byte[] fuzzData, MutationStrategy strategy) {
    return switch (strategy) {
      case NO_MUTATION -> signatureData;
      case BIT_FLIP -> applyBitFlipMutation(signatureData, fuzzData);
      case SIGNATURE_MALLEABILITY -> applySignatureMalleabilityMutation(signatureData);
      case ARITHMETIC_MUTATION -> applyArithmeticMutation(signatureData, fuzzData);
      default -> signatureData;
    };
  }

  /** Applies bit flip mutation to signature data, focusing on signature components. */
  private byte[] applyBitFlipMutation(byte[] signatureData, byte[] fuzzData) {
    if (fuzzData.length < 3) {
      return signatureData;
    }

    byte[] mutated = Arrays.copyOf(signatureData, signatureData.length);

    // Select which component to flip: 0=r, 1=s, 2=qx, 3=qy (skip message hash)
    int component = (fuzzData[1] & 0xFF) % 4;
    int componentOffset =
        switch (component) {
          case 0 -> 32; // r starts at byte 32
          case 1 -> 64; // s starts at byte 64
          case 2 -> 96; // qx starts at byte 96
          case 3 -> 128; // qy starts at byte 128
          default -> 32;
        };

    // Select which bit within the 32-byte component to flip
    int byteWithinComponent = (fuzzData[2] & 0xFF) % 32;
    int bitIndex = fuzzData.length > 3 ? (fuzzData[3] & 0x07) : ((fuzzData[2] >> 5) & 0x07);

    int targetByteIndex = componentOffset + byteWithinComponent;
    mutated[targetByteIndex] ^= (1 << bitIndex);

    return mutated;
  }

  /** Applies signature malleability mutation (s → n - s). */
  private byte[] applySignatureMalleabilityMutation(byte[] signatureData) {
    byte[] mutated = Arrays.copyOf(signatureData, signatureData.length);

    // Extract s value (bytes 64-95)
    BigInteger s = new BigInteger(1, Arrays.copyOfRange(signatureData, 64, 96));

    // Apply malleability: s' = n - s
    BigInteger malleableS = N.subtract(s);

    // Write back the malleable s value
    MutableBytes mutableData = MutableBytes.wrap(mutated);
    writeBigIntegerAs32Bytes(mutableData, 64, malleableS);

    return mutated;
  }

  /** Applies arithmetic mutations to signature components. */
  private byte[] applyArithmeticMutation(byte[] signatureData, byte[] fuzzData) {
    if (fuzzData.length < 3) {
      return signatureData;
    }

    byte[] mutated = Arrays.copyOf(signatureData, signatureData.length);

    // Select which component to mutate: 0=hash, 1=r, 2=s, 3=qx, 4=qy
    int component = (fuzzData[1] & 0xFF) % 5;
    int operation = (fuzzData[2] & 0xFF) % 4;

    int offset = getComponentOffset(component);
    BigInteger original = new BigInteger(1, Arrays.copyOfRange(mutated, offset, offset + 32));
    BigInteger modified = applyArithmeticOperation(original, operation, fuzzData);

    MutableBytes mutableData = MutableBytes.wrap(mutated);
    writeBigIntegerAs32Bytes(mutableData, offset, modified);

    return mutated;
  }

  /** Gets the byte offset for a signature component. */
  private int getComponentOffset(int component) {
    return switch (component) {
      case 0 -> 0; // hash
      case 1 -> 32; // r
      case 2 -> 64; // s
      case 3 -> 96; // qx
      case 4 -> 128; // qy
      default -> 0;
    };
  }

  /** Applies an arithmetic operation to a BigInteger value. */
  private BigInteger applyArithmeticOperation(BigInteger value, int operation, byte[] fuzzData) {
    int delta = fuzzData.length > 3 ? (fuzzData[3] & 0xFF) : 1;

    return switch (operation) {
      case 0 -> value.add(BigInteger.valueOf(delta)); // Add
      case 1 -> value.subtract(BigInteger.valueOf(delta)); // Subtract
      case 2 -> value.multiply(BigInteger.valueOf(2 + (delta % 8))); // Multiply
      case 3 -> { // XOR
        byte[] bytes = value.toByteArray();
        for (int i = 0; i < bytes.length; i++) {
          bytes[i] ^= (byte) delta;
        }
        yield new BigInteger(1, bytes);
      }
      default -> value;
    };
  }

  /** Generates synthetic input for testing edge cases and boundary conditions. */
  private byte[] generateSyntheticInput(byte[] fuzzData, MutationStrategy strategy) {
    return switch (strategy) {
      case BOUNDARY_VALUES -> generateBoundaryValueInput(fuzzData);
      case CURVE_ATTACKS -> generateCurveAttackInput(fuzzData);
      case POINT_MANIPULATION -> generatePointManipulationInput(fuzzData);
      case BOUNCYCASTLE_BYPASS -> generateBouncyCastleBypassInput(fuzzData);
      default -> new byte[160];
    };
  }

  /** Generates input with boundary values for testing edge cases. */
  private byte[] generateBoundaryValueInput(byte[] fuzzData) {
    MutableBytes input = MutableBytes.create(160);

    // Random message hash
    input.set(0, generateRandomBytes32());

    if (fuzzData.length < 2) {
      return input.toArray();
    }

    int boundaryType = (fuzzData[1] & 0xFF) % 13; // Increased to include valid signature test

    switch (boundaryType) {
      case 0: // r = 0 (invalid)
        input.set(32, Bytes32.ZERO);
        break;
      case 1: // r = n-1 (valid boundary)
        writeBigIntegerAs32Bytes(input, 32, N.subtract(BigInteger.ONE));
        break;
      case 2: // r = n (invalid)
        writeBigIntegerAs32Bytes(input, 32, N);
        break;
      case 3: // s = 0 (invalid)
        input.set(64, Bytes32.ZERO);
        break;
      case 4: // s = n-1 (valid boundary)
        writeBigIntegerAs32Bytes(input, 64, N.subtract(BigInteger.ONE));
        break;
      case 5: // s = n (invalid)
        writeBigIntegerAs32Bytes(input, 64, N);
        break;
      case 6: // qx = p-1 (valid boundary)
        writeBigIntegerAs32Bytes(input, 96, P.subtract(BigInteger.ONE));
        break;
      case 7: // qy = p-1 (valid boundary)
        writeBigIntegerAs32Bytes(input, 128, P.subtract(BigInteger.ONE));
        break;
      case 8: // r = -1 (negative, invalid - tests r.signum() <= 0)
        writeBigIntegerAs32Bytes(input, 32, BigInteger.valueOf(-1));
        break;
      case 9: // s = -1 (negative, invalid - tests s.signum() <= 0)
        writeBigIntegerAs32Bytes(input, 64, BigInteger.valueOf(-1));
        break;
      case 10: // qx = -1 (negative, invalid - tests qx.signum() < 0)
        writeBigIntegerAs32Bytes(input, 96, BigInteger.valueOf(-1));
        break;
      case 11: // qy = -1 (negative, invalid - tests qy.signum() < 0)
        writeBigIntegerAs32Bytes(input, 128, BigInteger.valueOf(-1));
        break;
      case 12: // Complete valid signature (tests all conditions false branch)
        // Generate a completely valid signature that should pass all checks
        KeyPair validKeyPair = javaSignatureAlgorithm.generateKeyPair();
        Bytes32 validMessageHash = generateRandomBytes32();
        SECPSignature validSignature = javaSignatureAlgorithm.sign(validMessageHash, validKeyPair);

        // Set all components to valid values
        input.set(0, validMessageHash);
        writeBigIntegerAs32Bytes(input, 32, validSignature.getR());
        writeBigIntegerAs32Bytes(input, 64, validSignature.getS());
        Bytes validPubKey = validKeyPair.getPublicKey().getEncodedBytes();
        input.set(96, validPubKey.slice(0, 32)); // qx
        input.set(128, validPubKey.slice(32, 32)); // qy
        break;
    }

    // Fill remaining fields with valid random data (skip for case 12 as it's already complete)
    if (boundaryType != 12) {
      // Map boundaryType to the correct field index to skip
      int skipField = -1; // -1 means don't skip any field
      if (boundaryType <= 2 || boundaryType == 8) {
        skipField = 1; // Skip r field for boundaryType 0-2, 8
      } else if (boundaryType <= 5 || boundaryType == 9) {
        skipField = 2; // Skip s field for boundaryType 3-5, 9
      } else if (boundaryType == 6 || boundaryType == 10) {
        skipField = 3; // Skip qx field for boundaryType 6, 10
      } else if (boundaryType == 7 || boundaryType == 11) {
        skipField = 4; // Skip qy field for boundaryType 7, 11
      }
      fillRemainingFields(input, skipField);
    }

    return input.toArray();
  }

  /** Generates input for testing curve-specific attacks. */
  private byte[] generateCurveAttackInput(byte[] fuzzData) {
    MutableBytes input = MutableBytes.create(160);

    // Random message hash
    input.set(0, generateRandomBytes32());

    if (fuzzData.length < 2) {
      return input.toArray();
    }

    int attackType = (fuzzData[1] & 0xFF) % 7;

    switch (attackType) {
      case 0: // Point at infinity (0, 0)
        input.set(96, Bytes32.ZERO);
        input.set(128, Bytes32.ZERO);
        break;

      case 1: // Random off-curve point
        input.set(96, generateRandomBytes32());
        input.set(128, generateRandomBytes32());
        break;

      case 2: // Generator point G
        byte[] gEncoded = G.getEncoded(false);
        input.set(96, Bytes.wrap(Arrays.copyOfRange(gEncoded, 1, 33)));
        input.set(128, Bytes.wrap(Arrays.copyOfRange(gEncoded, 33, 65)));
        break;

      case 3: // High-order point (close to n*G)
        ECPoint highOrderPoint = G.multiply(N.subtract(BigInteger.ONE));
        if (!highOrderPoint.isInfinity()) {
          byte[] pointEncoded = highOrderPoint.getEncoded(false);
          input.set(96, Bytes.wrap(Arrays.copyOfRange(pointEncoded, 1, 33)));
          input.set(128, Bytes.wrap(Arrays.copyOfRange(pointEncoded, 33, 65)));
        }
        break;

      case 4: // Coordinates at field boundary (invalid - qx,qy must be < P)
        writeBigIntegerAs32Bytes(
            input, 96, P.subtract(BigInteger.ONE)); // qx = P-1 (valid, should be < P)
        writeBigIntegerAs32Bytes(
            input, 128, P.subtract(BigInteger.ONE)); // qy = P-1 (valid, should be < P)
        break;

      case 5: // Coordinates beyond field boundary (invalid - qx,qy must be < P)
        writeBigIntegerAs32Bytes(input, 96, P); // qx = P (invalid, should be < P)
        writeBigIntegerAs32Bytes(input, 128, P); // qy = P (invalid, should be < P)
        break;

      case 6: // Negative coordinates (when interpreted as signed)
        byte[] negativeBytes = new byte[32];
        Arrays.fill(negativeBytes, (byte) 0xFF);
        input.set(96, Bytes.wrap(negativeBytes));
        input.set(128, Bytes.wrap(negativeBytes));
        break;
    }

    // Fill r and s with valid-looking values
    writeBigIntegerAs32Bytes(input, 32, generateRandomBigInteger(BigInteger.ONE, N));
    writeBigIntegerAs32Bytes(input, 64, generateRandomBigInteger(BigInteger.ONE, N));

    return input.toArray();
  }

  /**
   * Generates input for testing invalid signature-public key pairings. Creates a valid signature
   * for one key, then uses it with a different/invalid public key.
   */
  private byte[] generatePointManipulationInput(byte[] fuzzData) {
    // Generate a valid signature with a known key pair
    KeyPair originalKeyPair = javaSignatureAlgorithm.generateKeyPair();
    Bytes32 messageHash = generateRandomBytes32();
    SECPSignature signature = javaSignatureAlgorithm.sign(messageHash, originalKeyPair);

    // Now create input with the valid signature but a manipulated/different public key
    MutableBytes input = MutableBytes.create(160);
    input.set(0, messageHash);
    writeBigIntegerAs32Bytes(input, 32, signature.getR());
    writeBigIntegerAs32Bytes(input, 64, signature.getS());

    if (fuzzData.length < 2) {
      // Use a completely different random public key
      KeyPair differentKey = javaSignatureAlgorithm.generateKeyPair();
      Bytes differentPubKey = differentKey.getPublicKey().getEncodedBytes();
      input.set(96, differentPubKey.slice(0, 32));
      input.set(128, differentPubKey.slice(32, 32));
      return input.toArray();
    }

    int manipulationType = (fuzzData[1] & 0xFF) % 5;
    Bytes originalPubKey = originalKeyPair.getPublicKey().getEncodedBytes();
    BigInteger qx = originalPubKey.slice(0, 32).toUnsignedBigInteger();
    BigInteger qy = originalPubKey.slice(32, 32).toUnsignedBigInteger();

    switch (manipulationType) {
      case 0: // Use negated y coordinate (different point on curve)
        writeBigIntegerAs32Bytes(input, 96, qx);
        writeBigIntegerAs32Bytes(input, 128, P.subtract(qy).mod(P));
        break;

      case 1: // Use swapped x and y coordinates (likely off-curve)
        writeBigIntegerAs32Bytes(input, 96, qy);
        writeBigIntegerAs32Bytes(input, 128, qx);
        break;

      case 2: // Use a completely different random key
        KeyPair differentKey = javaSignatureAlgorithm.generateKeyPair();
        Bytes differentPubKey = differentKey.getPublicKey().getEncodedBytes();
        input.set(96, differentPubKey.slice(0, 32));
        input.set(128, differentPubKey.slice(32, 32));
        break;

      case 3: // Slightly modify coordinates (likely makes it off-curve)
        int delta = fuzzData.length > 2 ? (fuzzData[2] & 0xFF) : 1;
        writeBigIntegerAs32Bytes(input, 96, qx.add(BigInteger.valueOf(delta)).mod(P));
        writeBigIntegerAs32Bytes(input, 128, qy.add(BigInteger.valueOf(delta)).mod(P));
        break;

      case 4: // Use coordinates beyond field boundary (invalid)
        writeBigIntegerAs32Bytes(input, 96, P.add(BigInteger.valueOf(1)));
        writeBigIntegerAs32Bytes(input, 128, P.add(BigInteger.valueOf(1)));
        break;
    }

    return input.toArray();
  }

  /** Generates input for testing BouncyCastle validation bypass scenarios. */
  private byte[] generateBouncyCastleBypassInput(byte[] fuzzData) {
    MutableBytes input = MutableBytes.create(160);

    // Random message hash
    input.set(0, generateRandomBytes32());

    if (fuzzData.length < 2) {
      return input.toArray();
    }

    int bypassType = (fuzzData[1] & 0xFF) % 5;

    switch (bypassType) {
      case 0: // True infinity point (0, 0) - BouncyCastle's implIsValid returns true
        input.set(96, Bytes32.ZERO);
        input.set(128, Bytes32.ZERO);
        break;

      case 1: // Coordinates exactly at field prime p
        writeBigIntegerAs32Bytes(input, 96, P);
        writeBigIntegerAs32Bytes(input, 128, P);
        break;

      case 2: // Coordinates just above field prime
        writeBigIntegerAs32Bytes(input, 96, P.add(BigInteger.ONE));
        writeBigIntegerAs32Bytes(input, 128, P.add(BigInteger.ONE));
        break;

      case 3: // Large coordinates that might overflow
        BigInteger largeValue = BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE);
        writeBigIntegerAs32Bytes(input, 96, largeValue);
        writeBigIntegerAs32Bytes(input, 128, largeValue);
        break;

      case 4: // Small non-zero coordinates that might be misinterpreted as infinity
        writeBigIntegerAs32Bytes(
            input, 96, BigInteger.valueOf(fuzzData.length > 2 ? (fuzzData[2] & 0xFF) : 1));
        writeBigIntegerAs32Bytes(
            input, 128, BigInteger.valueOf(fuzzData.length > 3 ? (fuzzData[3] & 0xFF) : 1));
        break;
    }

    // Fill r and s with valid-looking values
    writeBigIntegerAs32Bytes(input, 32, generateRandomBigInteger(BigInteger.ONE, N));
    writeBigIntegerAs32Bytes(input, 64, generateRandomBigInteger(BigInteger.ONE, N));

    return input.toArray();
  }

  /**
   * Verifies a signature test case with both Java and Native implementations. Checks for
   * discrepancies between the two implementations.
   *
   * <p>THREAD SAFETY: This method uses static toggles (disableNativeBoringSSL() and
   * maybeEnableNativeBoringSSL()) to switch between implementations. It is designed for
   * single-threaded fuzzing ONLY. The javafuzz framework executes fuzz() calls sequentially, not in
   * parallel, making this safe.
   */
  private void verifySignatureWithBothImplementations(SignatureTestCase testCase) {
    // First, validate gasRequirement method
    validateGasRequirement(testCase.signatureData);

    // Verify with Java implementation
    P256VerifyPrecompiledContract.disableNativeBoringSSL();
    PrecompiledContract.PrecompileContractResult javaResult =
        verificationContractJava.computePrecompile(testCase.signatureData, messageFrame);

    // Verify with Native implementation
    P256VerifyPrecompiledContract.maybeEnableNativeBoringSSL();
    PrecompiledContract.PrecompileContractResult nativeResult =
        verificationContractNative.computePrecompile(testCase.signatureData, messageFrame);

    // Check for discrepancies
    if (!javaResult.output().equals(nativeResult.output())) {
      String errorMsg =
          String.format(
              "VERIFICATION DISCREPANCY! "
                  + "Generation: %s, Strategy: %s, "
                  + "JavaVerify: %s, NativeVerify: %s, "
                  + "Input: %s",
              testCase.generationMethod,
              testCase.strategy.name(),
              javaResult.output().toHexString(),
              nativeResult.output().toHexString(),
              testCase.signatureData.toHexString());
      throw new AssertionError(errorMsg);
    }

    // Additional validation for obviously invalid inputs
    validateResultSanity(testCase.signatureData, javaResult.output(), testCase.strategy);
  }

  /**
   * Cross-validates that signatures generated by different implementations produce the same
   * verification results.
   */
  private void crossValidateSignatures(
      SignatureTestCase javaGenerated, SignatureTestCase nativeGenerated) {
    // Both valid signatures should verify successfully with both implementations
    P256VerifyPrecompiledContract.disableNativeBoringSSL();
    PrecompiledContract.PrecompileContractResult javaGenJavaVerify =
        verificationContractJava.computePrecompile(javaGenerated.signatureData, messageFrame);
    P256VerifyPrecompiledContract.maybeEnableNativeBoringSSL();
    PrecompiledContract.PrecompileContractResult nativeGenJavaVerify =
        verificationContractNative.computePrecompile(nativeGenerated.signatureData, messageFrame);

    // For valid signatures (NO_MUTATION), both should succeed
    if (!javaGenJavaVerify.output().equals(VALID_SIGNATURE_RESULT)
        || !nativeGenJavaVerify.output().equals(VALID_SIGNATURE_RESULT)) {
      String errorMsg =
          String.format(
              "VALID SIGNATURE FAILED! JavaGen: %s, NativeGen: %s",
              javaGenJavaVerify.output().toHexString(), nativeGenJavaVerify.output().toHexString());
      throw new AssertionError(errorMsg);
    }
  }

  /** Validates that verification results are sensible for obviously invalid inputs. */
  private void validateResultSanity(Bytes input, Bytes result, MutationStrategy strategy) {
    if (isObviouslyInvalid(input)) {
      if (result.equals(VALID_SIGNATURE_RESULT)) {
        String errorMsg =
            String.format(
                "SANITY CHECK FAILED! Obviously invalid input returned VALID. "
                    + "Strategy: %s, Input: %s",
                strategy.name(), input.toHexString());
        throw new AssertionError(errorMsg);
      }
    }
  }

  /**
   * Validates the gasRequirement method returns exactly 6900 gas per EIP-7951. The precompile MUST
   * consume exactly 6900 gas regardless of input validity.
   */
  private void validateGasRequirement(Bytes input) {
    final long EXPECTED_GAS = 6900L; // EIP-7951 specified gas cost

    try {
      // Call gasRequirement to ensure line 119 coverage
      long gasRequired = verificationContractJava.gasRequirement(input);

      // Assert gas cost is exactly 6900 per EIP-7951
      if (gasRequired != EXPECTED_GAS) {
        throw new AssertionError(
            String.format(
                "Gas requirement MUST be %d per EIP-7951, but got: %d", EXPECTED_GAS, gasRequired));
      }
    } catch (AssertionError e) {
      throw e; // Re-throw assertion errors
    } catch (Exception e) {
      throw new AssertionError("gasRequirement threw unexpected exception: " + e.getMessage(), e);
    }
  }

  /** Checks if an input is obviously invalid and should never verify successfully. */
  private boolean isObviouslyInvalid(Bytes input) {
    if (input.size() != 160) {
      return true;
    }

    // Extract signature components
    BigInteger r = input.slice(32, 32).toUnsignedBigInteger();
    BigInteger s = input.slice(64, 32).toUnsignedBigInteger();
    BigInteger qx = input.slice(96, 32).toUnsignedBigInteger();
    BigInteger qy = input.slice(128, 32).toUnsignedBigInteger();

    // Check for obviously invalid conditions
    if (r.signum() <= 0 || r.compareTo(N) >= 0) return true; // r must be in (0, n)
    if (s.signum() <= 0 || s.compareTo(N) >= 0) return true; // s must be in (0, n)
    if (qx.signum() < 0 || qx.compareTo(P) >= 0) return true; // qx must be in [0, p)
    if (qy.signum() < 0 || qy.compareTo(P) >= 0) return true; // qy must be in [0, p)
    if (qx.signum() == 0 && qy.signum() == 0) return true; // Point at infinity

    return false;
  }

  /** Fills remaining fields in the input with valid random data. */
  private void fillRemainingFields(MutableBytes input, int skipField) {
    for (int field = 0; field < 5; field++) {
      if (field == skipField) continue;

      int offset = getComponentOffset(field);

      if (field == 0) { // message hash - can be anything
        input.set(offset, generateRandomBytes32());
      } else if (field == 1 || field == 2) { // r, s - must be in (0, n)
        writeBigIntegerAs32Bytes(input, offset, generateRandomBigInteger(BigInteger.ONE, N));
      } else { // qx, qy - must be in [0, p)
        writeBigIntegerAs32Bytes(input, offset, generateRandomBigInteger(BigInteger.ZERO, P));
      }
    }
  }

  /** Generates a random 32-byte value. */
  private Bytes32 generateRandomBytes32() {
    byte[] bytes = new byte[32];
    random.nextBytes(bytes);
    return Bytes32.wrap(bytes);
  }

  /** Generates a random BigInteger in the specified range [min, max). */
  private BigInteger generateRandomBigInteger(BigInteger min, BigInteger max) {
    if (max.equals(min)) {
      return min;
    }

    BigInteger range = max.subtract(min);
    BigInteger randomValue;

    do {
      randomValue = new BigInteger(range.bitLength(), random);
    } while (randomValue.compareTo(range) >= 0);

    return randomValue.add(min);
  }

  /** Writes a BigInteger as exactly 32 bytes (big-endian, left-padded with zeros). */
  private static void writeBigIntegerAs32Bytes(MutableBytes target, int offset, BigInteger value) {
    byte[] bytes = value.toByteArray();

    // Handle the case where BigInteger adds a leading zero for sign
    int sourceOffset = 0;
    int sourceLength = bytes.length;

    // Skip leading zero if present and array is 33 bytes
    if (bytes.length == 33 && bytes[0] == 0) {
      sourceOffset = 1;
      sourceLength = 32;
    } else if (bytes.length > 32) {
      // If still larger than 32 bytes, take the last 32 bytes
      sourceOffset = bytes.length - 32;
      sourceLength = 32;
    }

    // Clear the target area first
    for (int i = 0; i < 32; i++) {
      target.set(offset + i, (byte) 0);
    }

    // Copy bytes to the right position (right-aligned with left zero padding)
    int destOffset = offset + Math.max(0, 32 - sourceLength);
    for (int i = 0; i < Math.min(sourceLength, 32); i++) {
      target.set(destOffset + i, bytes[sourceOffset + i]);
    }
  }
}
