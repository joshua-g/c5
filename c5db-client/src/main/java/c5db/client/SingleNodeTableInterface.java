/*
 * Copyright (C) 2013  Ohm Data
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
package c5db.client;

import c5db.client.generated.Call;
import c5db.client.generated.GetRequest;
import c5db.client.generated.MultiRequest;
import c5db.client.generated.MutateRequest;
import c5db.client.generated.Response;
import c5db.client.generated.ScanRequest;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static c5db.client.generated.Call.Command.GET;
import static c5db.client.generated.Call.Command.MULTI;
import static c5db.client.generated.Call.Command.MUTATE;
import static c5db.client.generated.Call.Command.SCAN;


/**
 * The main client entry point for putting data into C5. Equivalent to HTablet from HBase.
 */
public class SingleNodeTableInterface implements TableInterface {
  private static final Logger LOG = LoggerFactory.getLogger(SingleNodeTableInterface.class);
  private final AtomicLong commandId = new AtomicLong(0);
  private final C5ConnectionManager c5ConnectionManager;
  public Channel channel;
  private MessageHandler handler;

  /**
   * C5Table is the main entry points for clients of C5DB
   */

  public SingleNodeTableInterface(String hostname, int port)
      throws InterruptedException, ExecutionException, TimeoutException {
    this(hostname, port, new C5NettyConnectionManager());
  }

  public SingleNodeTableInterface(String hostname, int port, C5ConnectionManager c5ConnectionManager)
      throws InterruptedException, ExecutionException, TimeoutException {
    // TODO Route data so we don't need to connect to meta
    this.c5ConnectionManager = c5ConnectionManager;
    this.channel = c5ConnectionManager.getOrCreateChannel(hostname, port);
    this.handler = channel.pipeline().get(FutureBasedMessageHandler.class);
  }

  @Override
  public ListenableFuture<Response> get(final GetRequest get) {
    return handler.call(new Call(GET, commandId.incrementAndGet(), get, null, null, null), channel);
  }

  @Override
  public ListenableFuture<Long> scan(ScanRequest scanRequest) {
    return handler.callScan(new Call(SCAN, commandId.incrementAndGet(), null, null, scanRequest, null), channel);
  }

  @Override
  public ListenableFuture<Response> mutate(MutateRequest mutateRequest) {
    return handler.call(new Call(MUTATE, commandId.incrementAndGet(), null, mutateRequest, null, null), channel);
  }

  @Override
  public ListenableFuture<Response> multiRequest(MultiRequest multiRequest) {
    return handler.call(new Call(MULTI, commandId.incrementAndGet(), null, null, null, multiRequest), channel);
  }

  @Override
  public void close() {
    try {
      c5ConnectionManager.close();
    } catch (InterruptedException e) {
      LOG.error("Unable to close, interrupted");
      e.printStackTrace();
      System.exit(1);
    }
  }

  public ListenableFuture<Response> bufferMutate(MutateRequest mutateRequest) {
    return handler.buffer(new Call(MUTATE, commandId.incrementAndGet(), null, mutateRequest, null, null), channel);
  }

  public void flushHandler() {
    channel.flush();
  }
}
