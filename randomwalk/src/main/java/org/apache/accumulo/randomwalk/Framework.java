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
package org.apache.accumulo.randomwalk;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.randomwalk.error.ErrorReport;
import org.apache.accumulo.randomwalk.error.InMemoryErrorReporter;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class Framework {
  
  private static final Logger log = Logger.getLogger(Framework.class);
  private HashMap<String,Node> nodes = new HashMap<String,Node>();
  private String configDir = null;
  
  private static final Framework INSTANCE = new Framework();
  
  /**
   * @return Singleton instance of Framework
   */
  public static Framework getInstance() {
    return INSTANCE;
  }
  
  public String getConfigDir() {
    return configDir;
  }
  
  public void setConfigDir(String confDir) {
    configDir = confDir;
  }
  
  /**
   * Run random walk framework
   * 
   * @param startName
   *          Full name of starting graph or test
   * @param state
   * @param confDir
   */
  public int run(String startName, State state, String confDir) {
    boolean errorsSeen = false;
    try {
      System.out.println("confDir " + confDir);
      setConfigDir(confDir);
      Node node = getNode(startName);
      node.visit(state, new Properties());
      
      
      for (Entry<ErrorReport,Long> errors : state.getErrorReporter().report()) {
        if (!errorsSeen) {
          errorsSeen = true;
        }
        
        log.error(errors.getKey() + " occurred " + errors.getValue() + " times.");
      }
    } catch (Exception e) {
      log.error("Error during random walk", e);
      return -1;
    }
    
    if (errorsSeen) {
      return 1;
    }
    
    return 0;
  }
  
  /**
   * Creates node (if it does not already exist) and inserts into map
   * 
   * @param id
   *          Name of node
   * @return Node specified by id
   * @throws Exception
   */
  public Node getNode(String id) throws Exception {
    
    // check for node in nodes
    if (nodes.containsKey(id)) {
      return nodes.get(id);
    }
    
    // otherwise create and put in nodes
    Node node = null;
    if (id.endsWith(".xml")) {
      node = new Module(new File(configDir + "modules/" + id));
    } else {
      node = (Test) Class.forName(id).newInstance();
    }
    nodes.put(id, node);
    return node;
  }
  
  static class Opts extends Help {
    @Parameter(names="--configDir", required=true, description="directory containing the test configuration")
    String configDir;
    @Parameter(names="--logDir", required=true, description="location of the local logging directory")
    String localLogPath;
    @Parameter(names="--logId", required=true, description="a unique log identifier (like a hostname, or pid)")
    String logId;
    @Parameter(names="--module", required=true, description="the name of the module to run")
    String module;
  }
  
  static class Help {
    @Parameter(names={"-h", "-?", "--help", "-help"}, help=true)
    public boolean help = false;
    
    public void parseArgs(String programName, String[] args, Object ... others) {
      JCommander commander = new JCommander();
      commander.addObject(this);
      for (Object other : others)
        commander.addObject(other);
      commander.setProgramName(programName);
      try {
        commander.parse(args);
      } catch (ParameterException ex) {
        commander.usage();
        exitWithError(ex.getMessage(), 1);
      }
      if (help) {
        commander.usage();
        exit(0);
      }
    }
    
    public void exit(int status) {
      System.exit(status);
    }
    
    public void exitWithError(String message, int status) {
      System.err.println(message);
      exit(status);
    }
  }
  
  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(Framework.class.getName(), args);

    Properties props = new Properties();
    FileInputStream fis = new FileInputStream(opts.configDir + "/randomwalk.conf");
    props.load(fis);
    fis.close();
    
    System.setProperty("localLog", opts.localLogPath + "/" + opts.logId);
    System.setProperty("nfsLog", props.getProperty("NFS_LOGPATH") + "/" + opts.logId);
    
    DOMConfigurator.configure(opts.configDir + "logger.xml");
    
    State state = new State(props, new InMemoryErrorReporter());
    
    int retval = getInstance().run(opts.module, state, opts.configDir);
    
    System.exit(retval);
  }
}
