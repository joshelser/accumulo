/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.client.security.tokens;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import javax.security.auth.DestroyFailedException;

import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Preconditions;

/**
 * Authentication token for kerberos authenticated users
 */
public class KerberosToken implements AuthenticationToken {

  private UserGroupInformation ugi;

  /**
   * Creates a token using the provided {@link ugi}, asserting that the UGI has kerberos credentials
   *
   * @param ugi
   *          The UGI to use when creating the token
   */
  public KerberosToken(UserGroupInformation ugi) {
    Preconditions.checkArgument(ugi.hasKerberosCredentials());
  }

  /**
   * Creates a token using the login user as returned by {@link UserGroupInformation#getLoginUser()}
   *
   * @throws IOException
   */
  public KerberosToken() throws IOException {
    this(UserGroupInformation.getLoginUser());
  }

  @Override
  public void write(DataOutput out) throws IOException {

  }

  @Override
  public void readFields(DataInput in) throws IOException {

  }

  @Override
  public synchronized void destroy() throws DestroyFailedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDestroyed() {
    return false;
  }

  @Override
  public void init(Properties properties) {

  }

  @Override
  public Set<TokenProperty> getProperties() {
    return Collections.emptySet();
  }

  @Override
  public AuthenticationToken clone() {
    return new KerberosToken(ugi);
  }

}
