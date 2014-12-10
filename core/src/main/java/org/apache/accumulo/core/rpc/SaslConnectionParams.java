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
package org.apache.accumulo.core.rpc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.security.sasl.Sasl;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;

/**
 * Connection parameters for setting up a TSaslTransportFactory
 */
public class SaslConnectionParams {
  public enum QualityOfProtection {
    AUTH("auth"),
    AUTH_INT("auth-int"),
    AUTH_CONF("auth-conf");

    private final String quality;

    private QualityOfProtection(String quality) {
      this.quality = quality;
    }

    public String getQuality() {
      return quality;
    }
  }

  private QualityOfProtection qop;
  private String kerberosServerPrimary;
  private final Map<String,String> saslProperties;

  private SaslConnectionParams() {
    saslProperties = new HashMap<>();
  }

  /**
   * Generate an SaslConnectionParams instance given the provided AccumuloConfiguration. If SASL is not being used, a null object will be returned.
   *
   * @param conf
   * @return
   */
  public static SaslConnectionParams forConfig(AccumuloConfiguration conf) {
    if (!conf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      return null;
    }

    SaslConnectionParams params = new SaslConnectionParams();

    // Get the quality of protection to use
    final String qopValue = conf.get(Property.RPC_SASL_QOP);
    params.qop = QualityOfProtection.valueOf(qopValue);

    // Add in the SASL properties to a map so we don't have to repeatedly construct this map
    params.saslProperties.put(Sasl.QOP, params.qop.getQuality());

    // The primary from the KRB principal on each server (e.g. primary/instance@realm)
    params.kerberosServerPrimary = conf.get(Property.RPC_KERBEROS_PRIMARY);

    return params;
  }

  public Map<String,String> getSaslProperties() {
    return Collections.unmodifiableMap(saslProperties);
  }
  /**
   * The quality of protection used with SASL. See {@link Sasl#QOP} for more information.
   */
  public QualityOfProtection getQualityOfProtection() {
    return qop;
  }

  /**
   * The 'primary' component from the kerberos principals that servers are configured to use.
   */
  public String getKerberosServerPrimary() {
    return kerberosServerPrimary;
  }
}
