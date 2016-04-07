/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.datastore.v1beta3.client;

import com.google.datastore.v1beta3.AllocateIdsRequest;
import com.google.datastore.v1beta3.AllocateIdsResponse;
import com.google.datastore.v1beta3.BeginTransactionRequest;
import com.google.datastore.v1beta3.BeginTransactionResponse;
import com.google.datastore.v1beta3.CommitRequest;
import com.google.datastore.v1beta3.CommitResponse;
import com.google.datastore.v1beta3.LookupRequest;
import com.google.datastore.v1beta3.LookupResponse;
import com.google.datastore.v1beta3.RollbackRequest;
import com.google.datastore.v1beta3.RollbackResponse;
import com.google.datastore.v1beta3.RunQueryRequest;
import com.google.datastore.v1beta3.RunQueryResponse;
import com.google.rpc.Code;

import java.io.IOException;
import java.io.InputStream;

/**
 * Provides access to Cloud Datastore.
 */
public class Datastore {

  final RemoteRpc remoteRpc;

  Datastore(RemoteRpc remoteRpc) {
    this.remoteRpc = remoteRpc;
  }

  /**
   * Reset the RPC count.
   */
  public void resetRpcCount() {
    remoteRpc.resetRpcCount();
  }

  /**
   * Returns the number of RPC calls made since the client was created
   * or {@link #resetRpcCount} was called.
   */
  public int getRpcCount() {
    return remoteRpc.getRpcCount();
  }

  private DatastoreException invalidResponseException(String method, IOException exception) {
    return RemoteRpc.makeException(remoteRpc.getUrl(), method, Code.UNAVAILABLE,
        "Invalid response", exception);
  }

  public AllocateIdsResponse allocateIds(AllocateIdsRequest request) throws DatastoreException {
    try (InputStream is = remoteRpc.call("allocateIds", request)) {
      return AllocateIdsResponse.parseFrom(is);
    } catch (IOException exception) {
      throw invalidResponseException("allocateIds", exception);
    }
  }

  public BeginTransactionResponse beginTransaction(BeginTransactionRequest request)
      throws DatastoreException {
    try (InputStream is = remoteRpc.call("beginTransaction", request)) {
      return BeginTransactionResponse.parseFrom(is);
    } catch (IOException exception) {
      throw invalidResponseException("beginTransaction", exception);
    }
  }

  public CommitResponse commit(CommitRequest request) throws DatastoreException {
    try (InputStream is = remoteRpc.call("commit", request)) {
      return CommitResponse.parseFrom(is);
    } catch (IOException exception) {
      throw invalidResponseException("commit", exception);
    }
  }

  public LookupResponse lookup(LookupRequest request) throws DatastoreException {
    try (InputStream is = remoteRpc.call("lookup", request)) {
      return LookupResponse.parseFrom(is);
    } catch (IOException exception) {
      throw invalidResponseException("lookup", exception);
    }
  }

  public RollbackResponse rollback(RollbackRequest request) throws DatastoreException {
    try (InputStream is = remoteRpc.call("rollback", request)) {
      return RollbackResponse.parseFrom(is);
    } catch (IOException exception) {
      throw invalidResponseException("rollback", exception);
    }
  }

  public RunQueryResponse runQuery(RunQueryRequest request) throws DatastoreException {
    try (InputStream is = remoteRpc.call("runQuery", request)) {
      return RunQueryResponse.parseFrom(is);
    } catch (IOException exception) {
      throw invalidResponseException("runQuery", exception);
    }
  }
}
