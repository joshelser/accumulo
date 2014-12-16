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

import java.lang.reflect.Field;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.LoggingRunnable;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.thrift.UGIAssumingProcessor;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import com.google.common.net.HostAndPort;

public class TServerUtils {
  private static final Logger log = Logger.getLogger(TServerUtils.class);

  public static final ThreadLocal<String> clientAddress = new ThreadLocal<String>();

  /**
   * Start a server, at the given port, or higher, if that port is not available.
   *
   * @param portHintProperty
   *          the port to attempt to open, can be zero, meaning "any available port"
   * @param processor
   *          the service to be started
   * @param serverName
   *          the name of the class that is providing the service
   * @param threadName
   *          name this service's thread for better debugging
   * @return the server object created, and the port actually used
   * @throws UnknownHostException
   *           when we don't know our own address
   */
  public static ServerAddress startServer(AccumuloServerContext service, String address, Property portHintProperty, TProcessor processor, String serverName,
      String threadName, Property portSearchProperty, Property minThreadProperty, Property timeBetweenThreadChecksProperty, Property maxMessageSizeProperty)
      throws UnknownHostException {
    int portHint = service.getConfiguration().getPort(portHintProperty);
    int minThreads = 2;
    if (minThreadProperty != null)
      minThreads = service.getConfiguration().getCount(minThreadProperty);
    long timeBetweenThreadChecks = 1000;
    if (timeBetweenThreadChecksProperty != null)
      timeBetweenThreadChecks = service.getConfiguration().getTimeInMillis(timeBetweenThreadChecksProperty);
    long maxMessageSize = 10 * 1000 * 1000;
    if (maxMessageSizeProperty != null)
      maxMessageSize = service.getConfiguration().getMemoryInBytes(maxMessageSizeProperty);
    boolean portSearch = false;
    if (portSearchProperty != null)
      portSearch = service.getConfiguration().getBoolean(portSearchProperty);
    final boolean kerberosSecured = service.getConfiguration().getBoolean(Property.INSTANCE_RPC_SASL_ENABLED);
    if (kerberosSecured) {
      // Wrap the provided processor in our special processor which proxies the provided UGI on the logged-in UGI
      // Important that we have Timed -> UGIAssuming -> [provided] to make sure that the metrics are still reported
      // as the logged-in user.
      processor = new UGIAssumingProcessor(processor);
    }
    // create the TimedProcessor outside the port search loop so we don't try to register the same metrics mbean more than once
    TimedProcessor timedProcessor = new TimedProcessor(service.getConfiguration(), processor, serverName, threadName);
    Random random = new Random();
    for (int j = 0; j < 100; j++) {

      // Are we going to slide around, looking for an open port?
      int portsToSearch = 1;
      if (portSearch)
        portsToSearch = 1000;

      for (int i = 0; i < portsToSearch; i++) {
        int port = portHint + i;
        if (portHint != 0 && i > 0)
          port = 1024 + random.nextInt(65535 - 1024);
        if (port > 65535)
          port = 1024 + port % (65535 - 1024);
        try {
          HostAndPort addr = HostAndPort.fromParts(address, port);
          return TServerUtils.startTServer(addr, timedProcessor, serverName, threadName, minThreads,
              service.getConfiguration().getCount(Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE), timeBetweenThreadChecks, maxMessageSize,
              service.getServerSslParams(), service.getClientTimeoutInMillis(), kerberosSecured);
        } catch (TTransportException ex) {
          log.error("Unable to start TServer", ex);
          if (ex.getCause() == null || ex.getCause().getClass() == BindException.class) {
            // Note: with a TNonblockingServerSocket a "port taken" exception is a cause-less
            // TTransportException, and with a TSocket created by TSSLTransportFactory, it
            // comes through as caused by a BindException.
            log.info("Unable to use port " + port + ", retrying. (Thread Name = " + threadName + ")");
            UtilWaitThread.sleep(250);
          } else {
            // thrift is passing up a nested exception that isn't a BindException,
            // so no reason to believe retrying on a different port would help.
            log.error("Unable to start TServer", ex);
            break;
          }
        }
      }
    }
    throw new UnknownHostException("Unable to find a listen port");
  }

  /**
   * Create a NonBlockingServer with a custom thread pool that can dynamically resize itself.
   */
  public static ServerAddress createNonBlockingServer(HostAndPort address, TProcessor processor, final String serverName, String threadName,
      final int numThreads, final int numSTThreads, long timeBetweenThreadChecks, long maxMessageSize, boolean kerberosSecured) throws TTransportException {
    TNonblockingServerSocket transport = new TNonblockingServerSocket(new InetSocketAddress(address.getHostText(), address.getPort()));
    CustomNonBlockingServer.Args options = new CustomNonBlockingServer.Args(transport);
    options.protocolFactory(ThriftUtil.protocolFactory());
    options.transportFactory(ThriftUtil.transportFactory(maxMessageSize, kerberosSecured));
    options.maxReadBufferBytes = maxMessageSize;
    options.stopTimeoutVal(5);
    /*
     * Create our own very special thread pool.
     */
    final ThreadPoolExecutor pool = new SimpleThreadPool(numThreads, "ClientPool");
    // periodically adjust the number of threads we need by checking how busy our threads are
    SimpleTimer.getInstance(numSTThreads).schedule(new Runnable() {
      @Override
      public void run() {
        if (pool.getCorePoolSize() <= pool.getActiveCount()) {
          int larger = pool.getCorePoolSize() + Math.min(pool.getQueue().size(), 2);
          log.info("Increasing server thread pool size on " + serverName + " to " + larger);
          pool.setMaximumPoolSize(larger);
          pool.setCorePoolSize(larger);
        } else {
          if (pool.getCorePoolSize() > pool.getActiveCount() + 3) {
            int smaller = Math.max(numThreads, pool.getCorePoolSize() - 1);
            if (smaller != pool.getCorePoolSize()) {
              // ACCUMULO-2997 there is a race condition here... the active count could be higher by the time
              // we decrease the core pool size... so the active count could end up higher than
              // the core pool size, in which case everything will be queued... the increase case
              // should handle this and prevent deadlock
              log.info("Decreasing server thread pool size on " + serverName + " to " + smaller);
              pool.setCorePoolSize(smaller);
            }
          }
        }
      }
    }, timeBetweenThreadChecks, timeBetweenThreadChecks);
    options.executorService(pool);
    options.processorFactory(new TProcessorFactory(processor));
    if (address.getPort() == 0) {
      address = HostAndPort.fromParts(address.getHostText(), transport.getPort());
    }
    return new ServerAddress(new CustomNonBlockingServer(options), address);
  }

  public static TServer createThreadPoolServer(TServerTransport transport, TProcessor processor) {
    TThreadPoolServer.Args options = new TThreadPoolServer.Args(transport);
    options.protocolFactory(ThriftUtil.protocolFactory());
    options.transportFactory(ThriftUtil.transportFactory());
    options.processorFactory(new ClientInfoProcessorFactory(clientAddress, processor));
    return new TThreadPoolServer(options);
  }

  public static ServerAddress createSslThreadPoolServer(HostAndPort address, TProcessor processor, long socketTimeout, SslConnectionParams sslParams,
      boolean kerberosSecured)
      throws TTransportException {
    org.apache.thrift.transport.TServerSocket transport;
    try {
      transport = ThriftUtil.getServerSocket(address.getPort(), (int) socketTimeout, InetAddress.getByName(address.getHostText()), sslParams);
    } catch (UnknownHostException e) {
      throw new TTransportException(e);
    }
    if (address.getPort() == 0) {
      address = HostAndPort.fromParts(address.getHostText(), transport.getServerSocket().getLocalPort());
    }
    return new ServerAddress(createThreadPoolServer(transport, processor), address);
  }

  public static ServerAddress startTServer(AccumuloConfiguration conf, HostAndPort address, TProcessor processor, String serverName, String threadName, int numThreads, int numSTThreads,
 long timeBetweenThreadChecks, long maxMessageSize, SslConnectionParams sslParams, long sslSocketTimeout)
      throws TTransportException {
    final boolean kerberosSecured = conf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED);
    return startTServer(address, new TimedProcessor(conf, processor, serverName, threadName), serverName, threadName, numThreads, numSTThreads,
        timeBetweenThreadChecks, maxMessageSize, sslParams, sslSocketTimeout, kerberosSecured);
  }

  /**
   * Start the appropriate Thrift server (SSL or non-blocking server) for the given parameters. Non-null SSL parameters will cause an SSL server to be started.
   *
   * @return A ServerAddress encapsulating the Thrift server created and the host/port which it is bound to.
   */
  public static ServerAddress startTServer(HostAndPort address, TimedProcessor processor, String serverName, String threadName, int numThreads,
      int numSTThreads, long timeBetweenThreadChecks, long maxMessageSize, SslConnectionParams sslParams, long sslSocketTimeout, boolean kerberosSecured)
      throws TTransportException {

    ServerAddress serverAddress;
    if (sslParams != null) {
      serverAddress = createSslThreadPoolServer(address, processor, sslSocketTimeout, sslParams, kerberosSecured);
    } else {
      serverAddress = createNonBlockingServer(address, processor, serverName, threadName, numThreads, numSTThreads, timeBetweenThreadChecks, maxMessageSize,
          kerberosSecured);
    }
    final TServer finalServer = serverAddress.server;
    Runnable serveTask = new Runnable() {
      @Override
      public void run() {
        try {
          finalServer.serve();
        } catch (Error e) {
          Halt.halt("Unexpected error in TThreadPoolServer " + e + ", halting.");
        }
      }
    };
    serveTask = new LoggingRunnable(TServerUtils.log, serveTask);
    Thread thread = new Daemon(serveTask, threadName);
    thread.start();
    // check for the special "bind to everything address"
    if (serverAddress.address.getHostText().equals("0.0.0.0")) {
      // can't get the address from the bind, so we'll do our best to invent our hostname
      try {
        serverAddress = new ServerAddress(finalServer, HostAndPort.fromParts(InetAddress.getLocalHost().getHostName(), serverAddress.address.getPort()));
      } catch (UnknownHostException e) {
        throw new TTransportException(e);
      }
    }
    return serverAddress;
  }

  // Existing connections will keep our thread running: reach in with reflection and insist that they shutdown.
  public static void stopTServer(TServer s) {
    if (s == null)
      return;
    s.stop();
    try {
      Field f = s.getClass().getDeclaredField("executorService_");
      f.setAccessible(true);
      ExecutorService es = (ExecutorService) f.get(s);
      es.shutdownNow();
    } catch (Exception e) {
      TServerUtils.log.error("Unable to call shutdownNow", e);
    }
  }
}
