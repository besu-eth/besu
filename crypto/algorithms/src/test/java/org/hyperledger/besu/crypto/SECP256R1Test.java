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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.crypto.Hash.keccak256;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SECP256R1Test {

  private final List<SignTestVector> signTestVectors =
      List.of(
          new SignTestVector(
              "519b423d715f8b581f4fa8ee59f4771a5b44c8130b4e3eacca54a56dda72b464",
              "1ccbe91c075fc7f4f033bfa248db8fccd3565de94bbfb12f3c59ff46c271bf83ce4014c68811f9a21a1fdb2c0e6113e06db7ca93b7404e78dc7ccd5ca89a4ca9",
              "5905238877c77421f73e43ee3da6f2d9e2ccad5fc942dcec0cbd25482935faaf416983fe165b1a045ee2bcd2e6dca3bdf46c4310a7461f9a37960ca672d3feb5473e253605fb1ddfd28065b53cb5858a8ad28175bf9bd386a5e471ea7a65c17cc934a9d791e91491eb3754d03799790fe2d308d16146d5c9b0d0debd97d79ce8"),
          new SignTestVector(
              "0f56db78ca460b055c500064824bed999a25aaf48ebb519ac201537b85479813",
              "e266ddfdc12668db30d4ca3e8f7749432c416044f2d2b8c10bf3d4012aeffa8abfa86404a2e9ffe67d47c587ef7a97a7f456b863b4d02cfc6928973ab5b1cb39",
              "c35e2f092553c55772926bdbe87c9796827d17024dbb9233a545366e2e5987dd344deb72df987144b8c6c43bc41b654b94cc856e16b96d7a821c8ec039b503e3d86728c494a967d83011a0e090b5d54cd47f4e366c0912bc808fbb2ea96efac88fb3ebec9342738e225f7c7c2b011ce375b56621a20642b4d36e060db4524af1"),
          new SignTestVector(
              "e283871239837e13b95f789e6e1af63bf61c918c992e62bca040d64cad1fc2ef",
              "74ccd8a62fba0e667c50929a53f78c21b8ff0c3c737b0b40b1750b2302b0bde829074e21f3a0ef88b9efdf10d06aa4c295cc1671f758ca0e4cd108803d0f2614",
              "3c054e333a94259c36af09ab5b4ff9beb3492f8d5b4282d16801daccb29f70fe61a0b37ffef5c04cd1b70e85b1f549a1c4dc672985e50f43ea037efa9964f096b5f62f7ffdf8d6bfb2cc859558f5a393cb949dbd48f269343b5263dcdb9c556eca074f2e98e6d94c2c29a677afaf806edf79b15a3fcd46e7067b7669f83188ee"),
          new SignTestVector(
              "a3d2d3b7596f6592ce98b4bfe10d41837f10027a90d7bb75349490018cf72d07",
              "322f80371bf6e044bc49391d97c1714ab87f990b949bc178cb7c43b7c22d89e13c15d54a5cc6b9f09de8457e873eb3deb1fceb54b0b295da6050294fae7fd999",
              "0989122410d522af64ceb07da2c865219046b4c3d9d99b01278c07ff63eaf1039cb787ae9e2dd46436cc0415f280c562bebb83a23e639e476a02ec8cff7ea06cd12c86dcc3adefbf1a9e9a9b6646c7599ec631b0da9a60debeb9b3e19324977f3b4f36892c8a38671c8e1cc8e50fcd50f9e51deaf98272f9266fc702e4e57c30"),
          new SignTestVector(
              "53a0e8a8fe93db01e7ae94e1a9882a102ebd079b3a535827d583626c272d280d",
              "1bcec4570e1ec2436596b8ded58f60c3b1ebc6a403bc5543040ba829630572448af62a4c683f096b28558320737bf83b9959a46ad2521004ef74cf85e67494e1",
              "dc66e39f9bbfd9865318531ffe9207f934fa615a5b285708a5e9c46b7775150e818d7f24d2a123df3672fff2094e3fd3df6fbe259e3989dd5edfcccbe7d45e26a775a5c4329a084f057c42c13f3248e3fd6f0c76678f890f513c32292dd306eaa84a59abe34b16cb5e38d0e885525d10336ca443e1682aa04a7af832b0eee4e7"),
          new SignTestVector(
              "4af107e8e2194c830ffb712a65511bc9186a133007855b49ab4b3833aefc4a1d",
              "a32e50be3dae2c8ba3f5e4bdae14cf7645420d425ead94036c22dd6c4fc59e00d623bf641160c289d6742c6257ae6ba574446dd1d0e74db3aaa80900b78d4ae9",
              "600974e7d8c5508e2c1aab0783ad0d7c4494ab2b4da265c2fe496421c4df238b0be25f25659157c8a225fb03953607f7df996acfd402f147e37aee2f1693e3bf1c35eab3ae360a2bd91d04622ea47f83d863d2dfecb618e8b8bdc39e17d15d672eee03bb4ce2cc5cf6b217e5faf3f336fdd87d972d3a8b8a593ba85955cc9d71"),
          new SignTestVector(
              "78dfaa09f1076850b3e206e477494cddcfb822aaa0128475053592c48ebaf4ab",
              "8bcfe2a721ca6d753968f564ec4315be4857e28bef1908f61a366b1f03c974790f67576a30b8e20d4232d8530b52fb4c89cbc589ede291e499ddd15fe870ab96",
              "dfa6cb9b39adda6c74cc8b2a8b53a12c499ab9dee01b4123642b4f11af336a91a5c9ce0520eb2395a6190ecbf6169c4cba81941de8e76c9c908eb843b98ce95e0da29c5d4388040264e05e07030a577cc5d176387154eabae2af52a83e85c61c7c61da930c9b19e45d7e34c8516dc3c238fddd6e450a77455d534c48a152010b"),
          new SignTestVector(
              "80e692e3eb9fcd8c7d44e7de9f7a5952686407f90025a1d87e52c7096a62618a",
              "a88bc8430279c8c0400a77d751f26c0abc93e5de4ad9a4166357952fe041e7672d365a1eef25ead579cc9a069b6abc1b16b81c35f18785ce26a10ba6d1381185",
              "51d2547cbff92431174aa7fc7302139519d98071c755ff1c92e4694b58587ea560f72f32fc6dd4dee7d22bb7387381d0256e2862d0644cdf2c277c5d740fa089830eb52bf79d1e75b8596ecf0ea58a0b9df61e0c9754bfcd62efab6ea1bd216bf181c5593da79f10135a9bc6e164f1854bc8859734341aad237ba29a81a3fc8b"),
          new SignTestVector(
              "5e666c0db0214c3b627a8e48541cc84a8b6fd15f300da4dff5d18aec6c55b881",
              "1bc487570f040dc94196c9befe8ab2b6de77208b1f38bdaae28f9645c4d2bc3aec81602abd8345e71867c8210313737865b8aa186851e1b48eaca140320f5d8f",
              "558c2ac13026402bad4a0a83ebc9468e50f7ffab06d6f981e5db1d082098065bcff6f21a7a74558b1e8612914b8b5a0aa28ed5b574c36ac4ea5868432a62bb8ef0695d27c1e3ceaf75c7b251c65ddb268696f07c16d2767973d85beb443f211e6445e7fe5d46f0dce70d58a4cd9fe70688c035688ea8c6baec65a5fc7e2c93e8"),
          new SignTestVector(
              "f73f455271c877c4d5334627e37c278f68d143014b0a05aa62f308b2101c5308",
              "b8188bd68701fc396dab53125d4d28ea33a91daf6d21485f4770f6ea8c565dde423f058810f277f8fe076f6db56e9285a1bf2c2a1dae145095edd9c04970bc4a",
              "4d55c99ef6bd54621662c3d110c3cb627c03d6311393b264ab97b90a4b15214a5593ba2510a53d63fb34be251facb697c973e11b665cb7920f1684b0031b4dd370cb927ca7168b0bf8ad285e05e9e31e34bc24024739fdc10b78586f29eff94412034e3b606ed850ec2c1900e8e68151fc4aee5adebb066eb6da4eaa5681378e"),
          new SignTestVector(
              "b20d705d9bd7c2b8dc60393a5357f632990e599a0975573ac67fd89b49187906",
              "51f99d2d52d4a6e734484a018b7ca2f895c2929b6754a3a03224d07ae61166ce4737da963c6ef7247fb88d19f9b0c667cac7fe12837fdab88c66f10d3c14cad1",
              "f8248ad47d97c18c984f1f5c10950dc1404713c56b6ea397e01e6dd925e903b4fadfe2c9e877169e71ce3c7fe5ce70ee4255d9cdc26f6943bf48687874de64f6cf30a012512e787b88059bbf561162bdcc23a3742c835ac144cc14167b1bd6727e940540a9c99f3cbb41fb1dcb00d76dda04995847c657f4c19d303eb09eb48a"),
          new SignTestVector(
              "d4234bebfbc821050341a37e1240efe5e33763cbbb2ef76a1c79e24724e5a5e7",
              "8fb287f0202ad57ae841aea35f29b2e1d53e196d0ddd9aec24813d64c0922fb71f6daff1aa2dd2d6d3741623eecb5e7b612997a1039aab2e5cf2de969cfea573",
              "3b6ee2425940b3d240d35b97b6dcd61ed3423d8e71a0ada35d47b322d17b35ea0472f35edd1d252f87b8b65ef4b716669fc9ac28b00d34a9d66ad118c9d94e7f46d0b4f6c2b2d339fd6bcd351241a387cc82609057048c12c4ec3d85c661975c45b300cb96930d89370a327c98b67defaa89497aa8ef994c77f1130f752f94a4"),
          new SignTestVector(
              "b58f5211dff440626bb56d0ad483193d606cf21f36d9830543327292f4d25d8c",
              "68229b48c2fe19d3db034e4c15077eb7471a66031f28a980821873915298ba76303e8ee3742a893f78b810991da697083dd8f11128c47651c27a56740a80c24c",
              "c5204b81ec0a4df5b7e9fda3dc245f98082ae7f4efe81998dcaa286bd4507ca840a53d21b01e904f55e38f78c3757d5a5a4a44b1d5d4e480be3afb5b394a5d2840af42b1b4083d40afbfe22d702f370d32dbfd392e128ea4724d66a3701da41ae2f03bb4d91bb946c7969404cb544f71eb7a49eb4c4ec55799bda1eb545143a7"),
          new SignTestVector(
              "54c066711cdb061eda07e5275f7e95a9962c6764b84f6f1f3ab5a588e0a2afb1",
              "0a7dbb8bf50cb605eb2268b081f26d6b08e012f952c4b70a5a1e6e7d46af98bbf26dd7d799930062480849962ccf5004edcfd307c044f4e8f667c9baa834eeae",
              "72e81fe221fb402148d8b7ab03549f1180bcc03d41ca59d7653801f0ba853add1f6d29edd7f9abc621b2d548f8dbf8979bd16608d2d8fc3260b4ebc0dd42482481d548c7075711b5759649c41f439fad69954956c9326841ea6492956829f9e0dc789f73633b40f6ac77bcae6dfc7930cfe89e526d1684365c5b0be2437fdb01"),
          new SignTestVector(
              "34fa4682bf6cb5b16783adcd18f0e6879b92185f76d7c920409f904f522db4b1",
              "105d22d9c626520faca13e7ced382dcbe93498315f00cc0ac39c4821d0d737376c47f3cbbfa97dfcebe16270b8c7d5d3a5900b888c42520d751e8faf3b401ef4",
              "21188c3edd5de088dacc1076b9e1bcecd79de1003c2414c3866173054dc82dde85169baa77993adb20c269f60a5226111828578bcc7c29e6e8d2dae81806152c8ba0c6ada1986a1983ebeec1473a73a04795b6319d48662d40881c1723a706f516fe75300f92408aa1dc6ae4288d2046f23c1aa2e54b7fb6448a0da922bd7f34"));

  protected SECP256R1 secp256R1;

  protected static String suiteStartTime = null;
  protected static String suiteName = null;

  @BeforeAll
  public static void setTestSuiteStartTime() {
    suiteStartTime =
        LocalDateTime.now(ZoneId.systemDefault())
            .format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
    suiteName(SECP256R1Test.class);
  }

  @BeforeEach
  public void setUp() {
    secp256R1 = new SECP256R1();
  }

  public static void suiteName(final Class<?> clazz) {
    suiteName = clazz.getSimpleName() + "-" + suiteStartTime;
  }

  public static String suiteName() {
    return suiteName;
  }

  @Test
  void recoverPublicKeyFromSignature() {
    final SECPPrivateKey privateKey =
        secp256R1.createPrivateKey(
            new BigInteger("c85ef7d79691fe79573b1a7064c19c1a9819ebdbd1faaab1a8ec92344438aaf4", 16));
    final KeyPair keyPair = secp256R1.createKeyPair(privateKey);

    final Bytes data = Bytes.wrap("This is an example of a signed message.".getBytes(UTF_8));
    final Bytes32 dataHash = keccak256(data);
    final SECPSignature signature = secp256R1.sign(dataHash, keyPair);

    final SECPPublicKey recoveredPublicKey =
        secp256R1.recoverPublicKeyFromSignature(dataHash, signature).get();
    assertThat(recoveredPublicKey).hasToString(keyPair.getPublicKey().toString());
  }

  @Test
  void signatureGenerationVerificationAndPubKeyRecovery() {
    signTestVectors.forEach(
        signTestVector -> {
          final SECPPrivateKey privateKey =
              secp256R1.createPrivateKey(new BigInteger(signTestVector.privateKey(), 16));
          final BigInteger publicKeyBigInt = new BigInteger(signTestVector.publicKey(), 16);
          final SECPPublicKey publicKey = secp256R1.createPublicKey(publicKeyBigInt);
          final KeyPair keyPair = secp256R1.createKeyPair(privateKey);

          final Bytes32 dataHash = keccak256(Bytes.wrap(signTestVector.data().getBytes(UTF_8)));

          final SECPSignature signature = secp256R1.sign(dataHash, keyPair);
          assertThat(secp256R1.verify(dataHash, signature, publicKey)).isTrue();

          final BigInteger recoveredPubKeyBigInt =
              secp256R1.recoverFromSignature(
                  signature.getRecId(), signature.getR(), signature.getS(), dataHash);
          assertThat(recoveredPubKeyBigInt).isEqualTo(publicKeyBigInt);
        });
  }

  @Test
  void ecdhKeyAgreementIsSymmetric() {
    final KeyPair kp1 = secp256R1.generateKeyPair();
    final KeyPair kp2 = secp256R1.generateKeyPair();

    final Bytes32 secret1 =
        secp256R1.calculateECDHKeyAgreement(kp1.getPrivateKey(), kp2.getPublicKey());
    final Bytes32 secret2 =
        secp256R1.calculateECDHKeyAgreement(kp2.getPrivateKey(), kp1.getPublicKey());

    assertThat(secret1).isEqualTo(secret2);
  }

  @Test
  void ecdhKeyAgreementCompressedIsSymmetric() {
    final KeyPair kp1 = secp256R1.generateKeyPair();
    final KeyPair kp2 = secp256R1.generateKeyPair();

    final Bytes compressed1 =
        secp256R1.calculateECDHKeyAgreementCompressed(kp1.getPrivateKey(), kp2.getPublicKey());
    final Bytes compressed2 =
        secp256R1.calculateECDHKeyAgreementCompressed(kp2.getPrivateKey(), kp1.getPublicKey());

    assertThat(compressed1).isEqualTo(compressed2);
  }

  @Test
  void ecdhCompressedReturns33BytesWithCorrectPrefix() {
    final KeyPair kp1 = secp256R1.generateKeyPair();
    final KeyPair kp2 = secp256R1.generateKeyPair();

    final Bytes compressed =
        secp256R1.calculateECDHKeyAgreementCompressed(kp1.getPrivateKey(), kp2.getPublicKey());

    assertThat(compressed.size()).isEqualTo(33);
    final byte prefix = compressed.get(0);
    assertThat(prefix == 0x02 || prefix == 0x03).isTrue();
  }

  @Test
  void ecdhCompressedXCoordinateMatchesUncompressed() {
    final KeyPair kp1 = secp256R1.generateKeyPair();
    final KeyPair kp2 = secp256R1.generateKeyPair();

    final Bytes32 xOnly =
        secp256R1.calculateECDHKeyAgreement(kp1.getPrivateKey(), kp2.getPublicKey());
    final Bytes compressed =
        secp256R1.calculateECDHKeyAgreementCompressed(kp1.getPrivateKey(), kp2.getPublicKey());

    assertThat(compressed.slice(1, 32)).isEqualTo(xOnly);
  }

  @Test
  void invalidFileThrowsInvalidKeyPairException() throws Exception {
    final File tempFile = Files.createTempFile(suiteName(), ".keypair").toFile();
    tempFile.deleteOnExit();
    Files.writeString(tempFile.toPath(), "not valid");
    assertThatThrownBy(() -> KeyPairUtil.load(tempFile))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void invalidMultiLineFileThrowsInvalidIdException() throws Exception {
    final File tempFile = Files.createTempFile(suiteName(), ".keypair").toFile();
    tempFile.deleteOnExit();
    Files.writeString(tempFile.toPath(), "not\n\nvalid");
    assertThatThrownBy(() -> KeyPairUtil.load(tempFile))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private record SignTestVector(String privateKey, String publicKey, String data) {}
}
