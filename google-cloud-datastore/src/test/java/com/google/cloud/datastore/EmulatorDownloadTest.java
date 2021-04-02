/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.datastore;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.logging.Logger;
import org.junit.Ignore;
import org.junit.Test;

public class EmulatorDownloadTest {
  private static final Logger LOGGER = Logger.getLogger(EmulatorDownloadTest.class.getName());

  @Test
  public void testLDHEmulatorDownloadWithAuth() throws Exception {
    assertNotNull(System.getenv("DATASTORE_EMULATOR_URL"));
    LOGGER.info("DATASTORE_EMULATOR_URL = " + System.getenv("DATASTORE_EMULATOR_URL"));
    LocalDatastoreHelper helper = LocalDatastoreHelper.create();
    helper.start();
  }

  @Ignore
  @Test
  public void testEmulatorDownloadWithInjectedAccessToken() throws Exception {
    assertNotNull(System.getenv("FAKE_DATASTORE_EMULATOR_URL_WITH_AUTH"));
    assertNotNull(System.getenv("ACCESS_TOKEN"));
    URL emulatorUrl = new URL(System.getenv("FAKE_DATASTORE_EMULATOR_URL_WITH_AUTH"));
    String accessToken = System.getenv("ACCESS_TOKEN");
    LOGGER.info("FAKE_DATASTORE_EMULATOR_URL_WITH_AUTH = " + emulatorUrl);
    LOGGER.info("ACCESS_TOKEN = " + accessToken);
    File zipFile = new File(System.getProperty("java.io.tmpdir"), "datastore-emulator.zip");
    downloadFile(zipFile, emulatorUrl, accessToken);
    assertTrue(zipFile.exists());
  }

  @Ignore
  @Test
  public void testEmulatorDownload() throws Exception {
    assertNotNull(System.getenv("FAKE_DATASTORE_EMULATOR_URL_WITH_AUTH"));
    URL emulatorUrl = new URL(System.getenv("FAKE_DATASTORE_EMULATOR_URL_WITH_AUTH"));
    LOGGER.info("FAKE_DATASTORE_EMULATOR_URL_WITH_AUTH = " + emulatorUrl);
    GoogleCredentials googleCredentials = GoogleCredentials.getApplicationDefault().createScoped(
        "https://www.googleapis.com/auth/devstorage.read_only");
    googleCredentials.refreshIfExpired();
    String accessToken = googleCredentials.getAccessToken().getTokenValue();
    LOGGER.info("local accessToken = " + accessToken);
    File zipFile = new File(System.getProperty("java.io.tmpdir"), "datastore-emulator.zip");
    downloadFile(zipFile, emulatorUrl, accessToken);
    assertTrue(zipFile.exists());
  }

  private void downloadFile(File file, URL fileUrl, String accessToken) throws Exception {
    if (file.exists()) file.delete();
    URLConnection urlConnection = fileUrl.openConnection();
    urlConnection.setRequestProperty("Authorization", "Bearer " + accessToken);
    ReadableByteChannel rbc = Channels.newChannel(urlConnection.getInputStream());
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
    }
  }
}
