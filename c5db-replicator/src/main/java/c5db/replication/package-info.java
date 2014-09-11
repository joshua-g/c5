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

/**
 * This package contains functionality for replicating logs. A log in this context is any sequence of data
 * items; for instance, a write-ahead log. "Replication" here means functionally duplicating the information
 * in the log, in general on several different durable media hosted by different machines. The purposes of
 * replication are fault tolerance and availability.
 * <p>
 * The package contains an implementation of the {@link c5db.interfaces.ReplicationModule} interface:
 * ReplicatorService. A given ReplicatorService instance participates in the replication of zero to
 * many logs.
 * <p>
 * ReplicatorService keeps a separate instance of the implementation of the
 * {@link c5db.interfaces.replication.Replicator} interface for each quorum it participates in. Each such
 * implementation (e.g. ReplicatorInstance) is in charge of the logic and state for the replication of that
 * quorum, and that logic and state is separate from that of any other quorum supported by the same
 * ReplicatorService.
 */

package c5db.replication;
