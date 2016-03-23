/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;

/**
 * Created by peter on 12/02/2016.
 */
public enum NoDocumentContext implements DocumentContext {
    INSTANCE;

    @Override
    public boolean isMetaData() {
        return false;
    }

    @Override
    public void metaData(boolean metaData) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean isPresent() {
        return false;
    }

    @Override
    public boolean isData() {
        return false;
    }

    @Override
    public Wire wire() {
        return null;
    }

    @Override
    public void close() {

    }
}
