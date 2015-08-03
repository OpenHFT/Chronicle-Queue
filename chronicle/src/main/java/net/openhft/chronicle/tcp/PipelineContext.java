/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 */
package net.openhft.chronicle.tcp;

import net.openhft.chronicle.tcp.network.SessionDetailsProvider;
import net.openhft.lang.io.Bytes;

/**
 * Context to be passed through the pipeline of tcp handlers
 *
 * Author:  Ryan Lea
 */
public interface PipelineContext {

    void next(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider);

}
