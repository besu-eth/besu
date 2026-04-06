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

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.google.common.io.Resources;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Key pair util. */
public class KeyPairUtil {
  private static final Logger LOG = LoggerFactory.getLogger(KeyPairUtil.class);
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  /** Default constructor */
  private KeyPairUtil() {}

  /**
   * Load resource file string.
   *
   * @param resourcePath the resource path
   * @return the string
   */
  public static String loadResourceFile(final String resourcePath) {
    try {
      URL path = KeyPairUtil.class.getClassLoader().getResource(resourcePath);
      return Resources.toString(path, StandardCharsets.UTF_8).trim();
    } catch (Exception e) {
      throw new RuntimeException("Unable to load resource: " + resourcePath, e);
    }
  }

  /**
   * Load key pair from resource.
   *
   * @param resourcePath the resource path
   * @return the key pair
   */
  public static KeyPair loadKeyPairFromResource(final String resourcePath) {
    final KeyPair keyPair;
    String keyData = loadResourceFile(resourcePath);
    if (keyData == null || keyData.isEmpty()) {
      throw new IllegalArgumentException("Unable to load resource: " + resourcePath);
    }
    SECPPrivateKey privateKey =
        SIGNATURE_ALGORITHM.createPrivateKey(Bytes32.fromHexString((keyData)));
    keyPair = SIGNATURE_ALGORITHM.createKeyPair(privateKey);

    LOG.info("Loaded keyPair {} from {}", keyPair.getPublicKey().toString(), resourcePath);
    return keyPair;
  }

  /**
   * Load key pair.
   *
   * @param keyFile the key file
   * @return the key pair
   */
  public static KeyPair loadKeyPair(final File keyFile) {

    final KeyPair key;
    if (keyFile.exists()) {

      LOG.info("Attempting to load public key from {}", keyFile.getAbsolutePath());
      key = load(keyFile);
      LOG.info(
          "Loaded public key {} from {}", key.getPublicKey().toString(), keyFile.getAbsolutePath());
    } else {
      key = SIGNATURE_ALGORITHM.generateKeyPair();
      storeKeyFile(key, keyFile.getParentFile().toPath());

      LOG.info(
          "Generated new {} public key {} and stored it to {}",
          SIGNATURE_ALGORITHM.getCurveName(),
          key.getPublicKey().toString(),
          keyFile.getAbsolutePath());
    }
    return key;
  }

  /**
   * Load key pair.
   *
   * @param directory the directory
   * @return the key pair
   */
  public static KeyPair loadKeyPair(final Path directory) {
    return loadKeyPair(getDefaultKeyFile(directory));
  }

  /**
   * Store key file.
   *
   * @param keyPair the key pair
   * @param homeDirectory the home directory
   */
  public static void storeKeyFile(final KeyPair keyPair, final Path homeDirectory) {
    try {
      storeKeyPair(keyPair, getDefaultKeyFile(homeDirectory));
    } catch (IOException e) {
      throw new IllegalArgumentException("Cannot store generated private key.");
    }
  }

  /**
   * Gets default key file.
   *
   * @param directory the directory
   * @return the default key file
   */
  public static File getDefaultKeyFile(final Path directory) {
    return directory.resolve("key").toFile();
  }

  /**
   * Load key pair.
   *
   * @param file the file
   * @return the key pair
   */
  public static KeyPair load(final File file) {
    return SIGNATURE_ALGORITHM.createKeyPair(loadPrivateKey(file));
  }

  /**
   * Load secp private key.
   *
   * @param file the file
   * @return the secp private key
   */
  static SECPPrivateKey loadPrivateKey(final File file) {
    try {
      final List<String> info = Files.readAllLines(file.toPath());
      if (info.size() != 1) {
        throw new IllegalArgumentException("Supplied file does not contain valid keyPair pair.");
      }
      return SIGNATURE_ALGORITHM.createPrivateKey(Bytes32.fromHexString((info.get(0))));
    } catch (IOException ex) {
      throw new IllegalArgumentException("Supplied file does not contain valid keyPair pair.");
    }
  }

  /**
   * Store key pair.
   *
   * @param keyPair the key pair
   * @param file the file
   * @throws IOException the io exception
   */
  static void storeKeyPair(final KeyPair keyPair, final File file) throws IOException {
    final File privateKeyDir = file.getParentFile();
    privateKeyDir.mkdirs();
    final Path tempPath = Files.createTempFile(privateKeyDir.toPath(), ".tmp", "");
    Files.write(
        tempPath,
        keyPair.getPrivateKey().getEncodedBytes().toString().getBytes(StandardCharsets.UTF_8));
    Files.move(tempPath, file.toPath(), REPLACE_EXISTING, ATOMIC_MOVE);
  }
}
