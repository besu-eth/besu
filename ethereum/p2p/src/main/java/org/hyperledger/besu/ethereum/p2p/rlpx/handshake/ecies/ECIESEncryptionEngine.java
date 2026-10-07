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
package org.hyperledger.besu.ethereum.p2p.rlpx.handshake.ecies;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPublicKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;

import org.apache.tuweni.bytes.Bytes;
import org.bouncycastle.crypto.BufferedBlockCipher;
import org.bouncycastle.crypto.DataLengthException;
import org.bouncycastle.crypto.DerivationFunction;
import org.bouncycastle.crypto.DerivationParameters;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.DigestDerivationFunction;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.Mac;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.modes.SICBlockCipher;
import org.bouncycastle.crypto.params.IESWithCipherParameters;
import org.bouncycastle.crypto.params.KDFParameters;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.params.ParametersWithIV;
import org.bouncycastle.util.Arrays;
import org.bouncycastle.util.Pack;

/**
 * An <a href="https://en.wikipedia.org/wiki/Integrated_Encryption_Scheme">Integrated Encryption
 * Scheme</a> engine that implements the encryption and decryption logic behind the ECIES crypto
 * handshake during the RLPx connection establishment.
 *
 * <p>This class has been inspired by the <code>IESEngine</code> implementation in Bouncy Castle. It
 * has been modified heavily to accommodate our usage, yet the core logic remains unchanged. It
 * implements a peculiarity of the Ethereum encryption protocol: updating the encryption MAC with
 * the IV.
 */
public class ECIESEncryptionEngine {

  public static final int ENCRYPTION_OVERHEAD = 113;

  private static final byte[] IES_DERIVATION = new byte[0];
  private static final byte[] IES_ENCODING = new byte[0];
  private static final short CIPHER_BLOCK_SIZE = 16;
  private static final short CIPHER_KEY_SIZE_BITS = CIPHER_BLOCK_SIZE * 8;

  private static final IESWithCipherParameters PARAM =
      new IESWithCipherParameters(
          IES_DERIVATION, IES_ENCODING, CIPHER_KEY_SIZE_BITS, CIPHER_KEY_SIZE_BITS);
  private static final int CIPHER_KEY_SIZE = PARAM.getCipherKeySize();
  private static final int CIPHER_MAC_KEY_SIZE = PARAM.getMacKeySize();

  // Configure the components of the Integrated Encryption Scheme.
  private final Digest hash = new SHA256Digest();
  private final DerivationFunction kdf = new ECIESHandshakeKDFFunction();
  private final Mac mac = new HMac(new SHA256Digest());
  private final BufferedBlockCipher cipher =
      new BufferedBlockCipher(new SICBlockCipher(new AESEngine()));

  private final SECPPublicKey ephPubKey;
  private final byte[] iv;

  private ECIESEncryptionEngine(
      final Bytes agreedSecret, final SECPPublicKey ephPubKey, final byte[] iv) {
    this.ephPubKey = ephPubKey;
    this.iv = iv;

    // Initialise the KDF.
    this.kdf.init(new KDFParameters(agreedSecret.toArrayUnsafe(), PARAM.getDerivationV()));
  }

  /**
   * Creates a new engine for decryption.
   *
   * @param nodeKey An abstraction of the decrypting private key
   * @param ephPubKey The ephemeral public key extracted from the raw message.
   * @param iv The initialization vector extracted from the raw message.
   * @return An engine prepared for decryption.
   */
  public static ECIESEncryptionEngine forDecryption(
      final NodeKey nodeKey, final SECPPublicKey ephPubKey, final Bytes iv) {
    final byte[] ivb = iv.toArray();

    // Create parameters.
    final Bytes agreedSecret = nodeKey.calculateECDHKeyAgreement(ephPubKey);

    return new ECIESEncryptionEngine(agreedSecret, ephPubKey, ivb);
  }

  /**
   * Creates a new engine for encryption.
   *
   * <p>The generated IV and ephemeral public key are available via getters {@link #getIv()} and
   * {@link #getEphPubKey()}.
   *
   * @param pubKey The public key of the receiver.
   * @return An engine prepared for encryption.
   */
  public static ECIESEncryptionEngine forEncryption(final SECPPublicKey pubKey) {
    final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithmFactory.getInstance();

    // Create an ephemeral key pair for IES whose public key we can later append in the message.
    final KeyPair ephKeyPair = signatureAlgorithm.generateKeyPair();

    // Create random iv.
    final byte[] ivb = ECIESHandshaker.random(CIPHER_BLOCK_SIZE).toArray();

    return new ECIESEncryptionEngine(
        signatureAlgorithm.calculateECDHKeyAgreement(ephKeyPair.getPrivateKey(), pubKey),
        ephKeyPair.getPublicKey(),
        ivb);
  }

  /**
   * Encrypts the provided plaintext.
   *
   * @param in The plaintext.
   * @return The ciphertext.
   * @throws InvalidCipherTextException Thrown if an error occurred during encryption.
   */
  public Bytes encrypt(final Bytes in) throws InvalidCipherTextException {
    return Bytes.wrap(encrypt(in.toArray(), 0, in.size(), null));
  }

  public Bytes encrypt(final Bytes in, final byte[] macData) throws InvalidCipherTextException {
    return Bytes.wrap(encrypt(in.toArray(), 0, in.size(), macData));
  }

  private byte[] encrypt(final byte[] in, final int inOff, final int inLen, final byte[] macData)
      throws InvalidCipherTextException {
    final byte[] C;
    final byte[] K;
    final byte[] K1;
    final byte[] K2;

    int len;

    // Block cipher mode.
    K1 = new byte[CIPHER_KEY_SIZE / 8];
    K2 = new byte[CIPHER_MAC_KEY_SIZE / 8];
    K = new byte[K1.length + K2.length];

    kdf.generateBytes(K, 0, K.length);
    System.arraycopy(K, 0, K1, 0, K1.length);
    System.arraycopy(K, K1.length, K2, 0, K2.length);

    // Initialize the cipher with the IV.
    cipher.init(true, new ParametersWithIV(new KeyParameter(K1), iv));

    C = new byte[cipher.getOutputSize(inLen)];
    len = cipher.processBytes(in, inOff, inLen, C, 0);
    len += cipher.doFinal(C, len);

    // Convert the length of the encoding vector into a byte array.
    final byte[] P2 = PARAM.getEncodingV();

    // Apply the MAC.
    final byte[] T = new byte[mac.getMacSize()];

    final byte[] K2hash = new byte[hash.getDigestSize()];
    hash.reset();
    hash.update(K2, 0, K2.length);
    hash.doFinal(K2hash, 0);

    mac.init(new KeyParameter(K2hash));
    mac.update(iv, 0, iv.length);
    mac.update(C, 0, C.length);

    if (P2 != null) {
      mac.update(P2, 0, P2.length);
    }

    if (macData != null) {
      mac.update(macData, 0, macData.length);
    }

    mac.doFinal(T, 0);

    final byte[] Output = new byte[len + T.length];
    System.arraycopy(C, 0, Output, 0, len);
    System.arraycopy(T, 0, Output, len, T.length);

    return Output;
  }

  /**
   * Decrypts the provided ciphertext.
   *
   * @param in The ciphertext.
   * @return The plaintext.
   * @throws InvalidCipherTextException Thrown if an error occurred during decryption.
   */
  public Bytes decrypt(final Bytes in) throws InvalidCipherTextException {
    return Bytes.wrap(decrypt(in.toArray(), 0, in.size(), null));
  }

  public Bytes decrypt(final Bytes in, final byte[] commonMac) throws InvalidCipherTextException {
    return Bytes.wrap(decrypt(in.toArray(), 0, in.size(), commonMac));
  }

  private byte[] decrypt(
      final byte[] inEnc, final int inOff, final int inLen, final byte[] commonMac)
      throws InvalidCipherTextException {
    final byte[] M;
    final byte[] K;
    final byte[] K1;
    final byte[] K2;

    int len;

    // Ensure that the length of the input is greater than the MAC in bytes
    if (inLen <= mac.getMacSize()) {
      throw new InvalidCipherTextException("Length of input must be greater than the MAC");
    }

    // Block cipher mode.
    K1 = new byte[CIPHER_KEY_SIZE / 8];
    K2 = new byte[CIPHER_MAC_KEY_SIZE / 8];
    K = new byte[K1.length + K2.length];

    kdf.generateBytes(K, 0, K.length);
    System.arraycopy(K, 0, K1, 0, K1.length);
    System.arraycopy(K, K1.length, K2, 0, K2.length);

    // Use IV to initialize cipher.
    cipher.init(false, new ParametersWithIV(new KeyParameter(K1), iv));

    M = new byte[cipher.getOutputSize(inLen - mac.getMacSize())];
    len = cipher.processBytes(inEnc, inOff, inLen - mac.getMacSize(), M, 0);
    len += cipher.doFinal(M, len);

    // Convert the length of the encoding vector into a byte array.
    final byte[] P2 = PARAM.getEncodingV();

    // Verify the MAC.
    final int end = inOff + inLen;
    final byte[] T1 = Arrays.copyOfRange(inEnc, end - mac.getMacSize(), end);
    final byte[] T2 = new byte[T1.length];

    final byte[] K2hash = new byte[hash.getDigestSize()];
    hash.reset();
    hash.update(K2, 0, K2.length);
    hash.doFinal(K2hash, 0);

    mac.init(new KeyParameter(K2hash));
    mac.update(iv, 0, iv.length);
    mac.update(inEnc, inOff, inLen - T2.length);

    if (P2 != null) {
      mac.update(P2, 0, P2.length);
    }

    if (commonMac != null) {
      mac.update(commonMac, 0, commonMac.length);
    }

    mac.doFinal(T2, 0);

    if (!Arrays.constantTimeAreEqual(T1, T2)) {
      throw new InvalidCipherTextException("Invalid MAC.");
    }

    // Output the message.
    return Arrays.copyOfRange(M, 0, len);
  }

  /**
   * Returns the initialization vector.
   *
   * <p>When encrypting a payload this value is automatically generated and accessible via this
   * getter.
   *
   * @return The initialization vector in use.
   */
  public Bytes getIv() {
    return Bytes.wrap(iv);
  }

  /**
   * Returns the ephemeral public key.
   *
   * <p>When encrypting a payload this value is automatically generated and accessible via this
   * getter.
   *
   * @return The ephemeral public key.
   */
  public SECPPublicKey getEphPubKey() {
    return ephPubKey;
  }

  /**
   * Key generation function as defined in NIST SP 800-56A, but swapping the order of the digested
   * values (counter first, shared secret second) to comply with Ethereum's approach.
   *
   * <p>This class has been adapted from the <code>BaseKDFBytesGenerator</code> implementation of
   * Bouncy Castle.
   */
  private static class ECIESHandshakeKDFFunction implements DigestDerivationFunction {

    private static final int COUNTER_START = 1;
    private final Digest digest = new SHA256Digest();
    private final int digestSize = digest.getDigestSize();
    private byte[] shared;
    private byte[] iv;

    @Override
    public void init(final DerivationParameters param) {
      checkArgument(param instanceof KDFParameters, "unexpected expected KDF params type");

      final KDFParameters p = (KDFParameters) param;
      shared = p.getSharedSecret();
      iv = p.getIV();
    }

    /**
     * Returns the underlying digest.
     *
     * @return The digest.
     */
    @Override
    public Digest getDigest() {
      return digest;
    }

    /**
     * Fills <code>len</code> bytes of the output buffer with bytes generated from the derivation
     * function.
     *
     * @throws IllegalArgumentException If the size of the request will cause an overflow.
     * @throws DataLengthException If the out buffer is too small.
     */
    @Override
    public int generateBytes(final byte[] out, final int outOff, final int len)
        throws DataLengthException, IllegalArgumentException {
      checkArgument(len >= 0, "length to fill cannot be negative");

      if ((out.length - len) < outOff) {
        throw new DataLengthException("output buffer too small");
      }

      final int outLen = digest.getDigestSize();
      final int cThreshold = (len + outLen - 1) / outLen;
      final byte[] dig = new byte[digestSize];
      final byte[] C = Pack.intToBigEndian(COUNTER_START);
      int counterBase = 0; // COUNTER_START & ~0xFF is always zero
      int offset = outOff;
      int length = len;

      for (int i = 0; i < cThreshold; i++) {
        // Ethereum peculiarity: Ethereum requires digesting the counter and the shared secret is
        // inverse order
        // that of the standard BaseKDFBytesGenerator in Bouncy Castle.
        digest.update(C, 0, C.length);
        digest.update(shared, 0, shared.length);

        if (iv != null) {
          digest.update(iv, 0, iv.length);
        }

        digest.doFinal(dig, 0);

        if (length > outLen) {
          System.arraycopy(dig, 0, out, offset, outLen);
          offset += outLen;
          length -= outLen;
        } else {
          System.arraycopy(dig, 0, out, offset, length);
        }

        if (++C[3] == 0) {
          counterBase += 0x100;
          Pack.intToBigEndian(counterBase, C, 0);
        }
      }

      digest.reset();
      return length;
    }
  }
}
