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
package org.hyperledger.besu.tests.acceptance.dsl.node;

import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import okhttp3.OkHttpClient;

/**
 * Trust-all TLS plumbing used by the acceptance-test DSL when talking to a Besu node configured
 * with a self-signed certificate. Strictly for use inside acceptance tests — never wire this into
 * production paths.
 */
final class InsecureTlsClientFactory {

  private InsecureTlsClientFactory() {}

  static SSLSocketFactory insecureSocketFactory() {
    try {
      final SSLContext context = SSLContext.getInstance("TLS");
      context.init(null, new TrustManager[] {trustAllManager()}, null);
      return context.getSocketFactory();
    } catch (final GeneralSecurityException e) {
      throw new IllegalStateException(
          "Unable to build insecure SSL context for acceptance tests", e);
    }
  }

  static OkHttpClient insecureOkHttpClient() {
    final X509TrustManager trustManager = trustAllManager();
    try {
      final SSLContext context = SSLContext.getInstance("TLS");
      context.init(null, new TrustManager[] {trustManager}, null);
      return new OkHttpClient.Builder()
          .sslSocketFactory(context.getSocketFactory(), trustManager)
          .hostnameVerifier((hostname, session) -> true)
          .build();
    } catch (final GeneralSecurityException e) {
      throw new IllegalStateException(
          "Unable to build insecure OkHttp client for acceptance tests", e);
    }
  }

  private static X509TrustManager trustAllManager() {
    return new X509TrustManager() {
      @Override
      public void checkClientTrusted(final X509Certificate[] chain, final String authType) {}

      @Override
      public void checkServerTrusted(final X509Certificate[] chain, final String authType) {}

      @Override
      public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
      }
    };
  }
}
