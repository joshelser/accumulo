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
package org.apache.accumulo.server.rpc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.server.thrift.UGIAssumingProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TCredentialsUpdatingInvocationHandler<I> implements InvocationHandler {
  private static final Logger log = LoggerFactory.getLogger(TCredentialsUpdatingInvocationHandler.class);

  private final I instance;

  protected TCredentialsUpdatingInvocationHandler(final I serverInstance) {
    instance = serverInstance;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    updateArgs(args);

    return invokeMethod(method, args);
  }

  /**
   * Try to find a TCredentials object in the argument list, and, when the AuthenticationToken is a KerberosToken, set the principal from the SASL server as the
   * TCredentials principal. This ensures that users can't spoof a different principal into the Credentials than what they used to authenticate.
   */
  private void updateArgs(Object[] args) {
    // If we don't have at least two args
    if (args == null || args.length < 2) {
      return;
    }

    TCredentials tcreds = null;
    if (args[0] != null && args[0] instanceof TCredentials) {
      tcreds = (TCredentials) args[0];
    } else if (args[1] != null && args[1] instanceof TCredentials) {
      tcreds = (TCredentials) args[1];
    }

    // If we don't find a tcredentials in the first two positions
    if (null == tcreds) {
      log.trace("Did not find a TCredentials object in the first two positions of the argument list");
      return;
    }

    // If the authentication token isn't a KerberosToken
    if (tcreds.tokenClassName != KerberosToken.CLASS_NAME) {
      log.trace("Will not update principal on authentication tokens other than {}", KerberosToken.CLASS_NAME);
      return;
    }

    // The Accumulo principal extracted from the SASL transport
    final String principal = UGIAssumingProcessor.principal.get();

    if (null == principal) {
      log.debug("Found KerberosToken in TCredentials, but did not receive principal from Sasl processor");
      return;
    }

    log.debug("Updating principal in TCredentials to {}", principal);
    tcreds.principal = principal;
  }

  private Object invokeMethod(Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(instance, args);
    } catch (InvocationTargetException ex) {
      throw ex.getCause();
    }
  }

}
