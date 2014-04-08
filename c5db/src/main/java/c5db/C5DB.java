/*
 * Copyright (C) 2014  Ohm Data
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package c5db;

import c5db.discovery.BeaconService;
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.log.LogFileService;
import c5db.log.LogService;
import c5db.messages.generated.CommandReply;
import c5db.messages.generated.ModuleType;
import c5db.messages.generated.StartModule;
import c5db.messages.generated.StopModule;
import c5db.regionserver.RegionServerService;
import c5db.replication.ReplicatorService;
import c5db.tablet.TabletService;
import c5db.util.C5FiberFactory;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.FiberOnly;
import c5db.util.PoolFiberFactoryWithExecutor;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.protostuff.Message;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.Disposable;
import org.jetlang.core.RunnableExecutorImpl;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.jetlang.fibers.ThreadFiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.Consumer;


/**
 * Holds information about all other modules, can start/stop other modules, etc.
 * Knows the 'root' information about this server as well, such as NodeId, etc.
 * <p>
 * To shut down the 'server' module is to shut down the server.
 */
public class C5DB extends AbstractService implements C5Server {
  private static final Logger LOG = LoggerFactory.getLogger(C5DB.class);
  private String clusterName;

  public static void main(String[] args) throws Exception {

    String username = System.getProperty("user.name");

    // nodeId is random initially.  Then if provided on args, we take that.
    Random rnd0 = new Random();
    long nodeId = rnd0.nextLong();

    if (args.length > 0) {
      nodeId = Long.parseLong(args[0]);
    }

    String cfgPath = "/tmp/" + username + "/c5-" + Long.toString(nodeId);

    // use system properties for other config so we don't end up writing a whole command line
    // parse framework.
    String reqCfgPath = System.getProperty("c5.cfgPath");
    if (reqCfgPath != null) {
      cfgPath = reqCfgPath;
    }

    NioFileConfigDirectory cfgDir = new NioFileConfigDirectory(Paths.get(cfgPath));
    cfgDir.setNodeIdFile(Long.toString(nodeId));

    instance = new C5DB(cfgDir);
    instance.start();
    Random rnd = new Random();

    int regionServerPort;
    if (System.getProperties().containsKey("regionServerPort")) {
      regionServerPort = Integer.parseInt(System.getProperty("regionServerPort"));
    } else {
      regionServerPort = 8080 + rnd.nextInt(1000);
    }

    // issue startup commands here that are common/we always want:
    StartModule startLog = new StartModule(ModuleType.Log, 0, "");
    instance.getCommandChannel().publish(startLog);

    StartModule startBeacon = new StartModule(ModuleType.Discovery, 54333, "");
    instance.getCommandChannel().publish(startBeacon);


    StartModule startReplication = new StartModule(ModuleType.Replication, rnd.nextInt(30000) + 1024, "");
    instance.getCommandChannel().publish(startReplication);

    StartModule startTablet = new StartModule(ModuleType.Tablet, 0, "");
    instance.getCommandChannel().publish(startTablet);

    StartModule startRegionServer = new StartModule(ModuleType.RegionServer, regionServerPort, "");
    instance.getCommandChannel().publish(startRegionServer);
  }

  private static C5Server instance = null;


  public C5DB(NioFileConfigDirectory configDirectory) throws IOException {
    this.configDirectory = configDirectory;

    String data = configDirectory.getNodeId();
    long toNodeId = 0;
    if (data != null) {
      try {
        toNodeId = Long.parseLong(data);
      } catch (NumberFormatException ignored) {
        throw new RuntimeException("NodeId not set");
      }
    }

    if (toNodeId == 0) {
      throw new RuntimeException("NodeId not set");
    }

    this.nodeId = toNodeId;

    if (System.getProperties().containsKey(C5ServerConstants.CLUSTER_NAME_PROPERTY_NAME)) {
      this.clusterName = System.getProperty(C5ServerConstants.CLUSTER_NAME_PROPERTY_NAME);
    } else {
      this.clusterName = C5ServerConstants.LOCALHOST;
    }


//        String clusterNameData = configDirectory.getClusterName();
//        if (clusterNameData == null) {
//            clusterNameData = "the-cluster";
//            configDirectory.setClusterNameFile(clusterNameData);
//        }
//        this.clusterName = clusterNameData;

  }

  /**
   * Returns the server, but it will be null if you aren't running inside one.
   *
   * @return return a static instance of C5DB.
   */
  public static C5Server getServer() {
    return instance;
  }

  @Override
  public long getNodeId() {
    return nodeId;
  }

  @Override
  public ListenableFuture<C5Module> getModule(final ModuleType moduleType) {
    final SettableFuture<C5Module> future = SettableFuture.create();
    serverFiber.execute(() -> {

      // What happens iff the moduleRegistry has EMPTY?
      if (!moduleRegistry.containsKey(moduleType)) {
        // listen to the registration stream:
        final Disposable[] d = new Disposable[]{null};
        d[0] = getModuleStateChangeChannel().subscribe(serverFiber, message -> {
          if (message.state != State.RUNNING) {
            return;
          }

          if (message.module.getModuleType().equals(moduleType)) {
            future.set(message.module);

            assert d[0] != null;  // this is pretty much impossible because of how fibers work.
            d[0].dispose();
          }
        });
      }

      future.set(moduleRegistry.get(moduleType));
    });
    return future;
  }

  @Override
  public ImmutableMap<ModuleType, C5Module> getModules() throws ExecutionException, InterruptedException {
    final SettableFuture<ImmutableMap<ModuleType, C5Module>> future = SettableFuture.create();
    serverFiber.execute(() -> {
      future.set(ImmutableMap.copyOf(moduleRegistry));
    });
    return future.get();
  }

  @Override
  public ListenableFuture<ImmutableMap<ModuleType, C5Module>> getModules2() {
    final SettableFuture<ImmutableMap<ModuleType, C5Module>> future = SettableFuture.create();
    serverFiber.execute(() -> {
      future.set(ImmutableMap.copyOf(moduleRegistry));
    });
    return future;
  }

  /**
   * * Implementation ***
   */


  private Fiber serverFiber;
  private final NioFileConfigDirectory configDirectory;

  // The mapping between module name and the instance.
  private final Map<ModuleType, C5Module> moduleRegistry = new HashMap<>();

  private final long nodeId;

  private final Channel<Message<?>> commandChannel = new MemoryChannel<>();

  private PoolFiberFactory fiberPool;
  private NioEventLoopGroup bossGroup;
  private NioEventLoopGroup workerGroup;

  @Override
  public Channel<Message<?>> getCommandChannel() {
    return commandChannel;
  }

  public RequestChannel<Message<?>, CommandReply> commandRequests = new MemoryRequestChannel<>();

  @Override
  public RequestChannel<Message<?>, CommandReply> getCommandRequests() {
    return commandRequests;
  }

  private final Channel<ModuleStateChange> serviceRegisteredChannel = new MemoryChannel<>();

  @Override
  public Channel<ModuleStateChange> getModuleStateChangeChannel() {
    return serviceRegisteredChannel;
  }

  @Override
  public NioFileConfigDirectory getConfigDirectory() {
    return configDirectory;
  }

  @Override
  public boolean isSingleNodeMode() {
    return this.clusterName.equals(C5ServerConstants.LOCALHOST);
  }

  @Override
  public Channel<ConfigKeyUpdated> getConfigUpdateChannel() {

    // TODO this
    return null;
  }

  @Override
  public C5FiberFactory getFiberFactory(Consumer<Throwable> throwableConsumer) {
    return new PoolFiberFactoryWithExecutor(fiberPool,
        new ExceptionHandlingBatchExecutor(throwableConsumer));
  }

  @FiberOnly
  private void processCommandMessage(Message<?> msg) throws Exception {
    if (msg instanceof StartModule) {
      StartModule message = (StartModule) msg;
      startModule(message.getModule(), message.getModulePort(), message.getModuleArgv());
    } else if (msg instanceof StopModule) {
      StopModule message = (StopModule) msg;
      stopModule(message.getModule(), message.getHardStop(), message.getStopReason());
    }
  }

  @FiberOnly
  private void processCommandRequest(Request<Message<?>, CommandReply> request) {
    Message<?> r = request.getRequest();
    try {
      String stdout;

      if (r instanceof StartModule) {
        StartModule message = (StartModule) r;
        startModule(message.getModule(), message.getModulePort(), message.getModuleArgv());

        stdout = String.format("Module %s started", message.getModule());
      } else if (r instanceof StopModule) {
        StopModule message = (StopModule) r;

        stopModule(message.getModule(), message.getHardStop(), message.getStopReason());

        stdout = String.format("Module %s started", message.getModule());
      } else {
        CommandReply reply = new CommandReply(false,
            "",
            String.format("Unknown message type: %s", r.getClass()));
        request.reply(reply);
        return;
      }

      CommandReply reply = new CommandReply(true, stdout, "");
      request.reply(reply);

    } catch (Exception e) {
      CommandReply reply = new CommandReply(false, "", e.toString());
      request.reply(reply);
    }
  }

  private class ModuleStatePublisher implements Listener {
    private final C5Module module;

    private ModuleStatePublisher(C5Module module) {
      this.module = module;
    }

    @Override
    public void starting() {
      LOG.debug("Starting module {}", module);
      publishEvent(State.STARTING);
    }

    @Override
    public void running() {
      LOG.debug("Running module {}", module);
      publishEvent(State.RUNNING);
    }

    @Override
    public void stopping(State from) {
      LOG.debug("Stopping module {}", module);
      publishEvent(State.STOPPING);
    }

    @Override
    public void terminated(State from) {
      // TODO move this into a subscriber of ourselves?
      LOG.debug("Terminated module {}", module);
      moduleRegistry.remove(module.getModuleType());
      publishEvent(State.TERMINATED);
    }

    @Override
    public void failed(State from, Throwable failure) {
      LOG.debug("Failed module " + module, failure);
      publishEvent(State.FAILED);
    }

    private void publishEvent(State state) {
      ModuleStateChange p = new ModuleStateChange(module, state);
      getModuleStateChangeChannel().publish(p);
    }

  }

  @FiberOnly
  private boolean startModule(final ModuleType moduleType, final int modulePort, String moduleArgv) throws Exception {
    if (moduleRegistry.containsKey(moduleType)) {
      // already running, don't start twice?
      LOG.warn("Module {} already running", moduleType);
      throw new Exception("Cant start, running, module: " + moduleType);
    }

    switch (moduleType) {
      case Discovery: {
        Map<ModuleType, Integer> l = new HashMap<>();
        for (ModuleType name : moduleRegistry.keySet()) {
          l.put(name, moduleRegistry.get(name).port());
        }

        C5Module module = new BeaconService(this.nodeId, modulePort, fiberPool.create(), workerGroup, l, this);
        startServiceModule(module);
        break;
      }
      case Replication: {
        C5Module module = new ReplicatorService(bossGroup, workerGroup, modulePort, this);
        startServiceModule(module);
        break;
      }
      case Log: {
        C5Module module = new LogService(this);
        startServiceModule(module);

        break;
      }
      case Tablet: {
        C5Module module = new TabletService(this);
        startServiceModule(module);

        break;
      }
      case RegionServer: {
        C5Module module = new RegionServerService(bossGroup, workerGroup, modulePort, this);
        startServiceModule(module);

        break;
      }

      default:
        throw new Exception("No such module as " + moduleType);
    }

    return true;
  }

  private void startServiceModule(C5Module module) {
    LOG.info("Starting service {}", module.getModuleType());
    module.addListener(new ModuleStatePublisher(module), serverFiber);

    module.start();
    moduleRegistry.put(module.getModuleType(), module);
  }

  @FiberOnly
  private void stopModule(ModuleType moduleType, boolean hardStop, String stopReason) {
    Service theModule = moduleRegistry.get(moduleType);
    if (theModule == null) {
      LOG.debug("Cant stop module {}, not in registry", moduleType);
      return;
    }

    theModule.stop();
  }

  @Override
  protected void doStart() {
//        Path path;
//        path = Paths.get(getRandomPath());
//        RegistryFile registryFile;
    try {
//            registryFile = new RegistryFile(configDirectory.baseConfigPath);

      // TODO this should probably be done somewhere else.
      new LogFileService(configDirectory.getBaseConfigPath()).clearOldArchivedLogs(0);

//            if (existingRegister(registryFile)) {
//                recoverC5Server(conf, path, registryFile);
//            } else {
//                bootStrapRegions(conf, path, registryFile);
//            }
    } catch (IOException e) {
      notifyFailed(e);
    }


    try {
      serverFiber = new ThreadFiber(new RunnableExecutorImpl(), "C5-Server", false);
      fiberPool = new PoolFiberFactory(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
      bossGroup = new NioEventLoopGroup(1);
      workerGroup = new NioEventLoopGroup();

      commandChannel.subscribe(serverFiber, message -> {
        try {
          processCommandMessage(message);
        } catch (Exception e) {
          LOG.warn("exception during message processing", e);
        }
      });

      commandRequests.subscribe(serverFiber, this::processCommandRequest);

      serverFiber.start();

      notifyStarted();
    } catch (Exception e) {
      notifyFailed(e);
    }
  }


  @Override
  protected void doStop() {
    // stop module set.

    // TODO write any last minute persistent data to disk (is there any?)
    // note: guava docs recommend doing long-acting operations in separate thread

    serverFiber.dispose();
    fiberPool.dispose();

    notifyStopped();
  }

}
