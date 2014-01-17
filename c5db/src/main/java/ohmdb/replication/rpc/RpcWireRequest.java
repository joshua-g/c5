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
package ohmdb.replication.rpc;

import com.google.protobuf.Message;
import ohmdb.replication.generated.Raft;

/**
 * And RPC request from off the wire, from a remote sender.
 */
public class RpcWireRequest extends RpcMessage {

    public RpcWireRequest(long from, String quorumId, Message message) {
        super(0, from, quorumId, message);
    }

    public RpcWireRequest(Raft.RaftWireMessage wireMessage) {
        super(wireMessage);
    }
}
