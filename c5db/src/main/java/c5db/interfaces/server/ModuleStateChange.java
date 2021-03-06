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
package c5db.interfaces.server;

import c5db.interfaces.C5Module;
import com.google.common.util.concurrent.Service;

/**
 * Notification object about when a module has a state change.
 */
public class ModuleStateChange {
  public final C5Module module;
  public final Service.State state;

  @Override
  public String toString() {
    return "ModuleStateChange{" +
        "module=" + module +
        ", state=" + state +
        '}';
  }

  public ModuleStateChange(C5Module module, Service.State state) {
    this.module = module;
    this.state = state;
  }
}
