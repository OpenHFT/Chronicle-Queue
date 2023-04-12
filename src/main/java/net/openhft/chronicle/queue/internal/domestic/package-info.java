/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This package and any and all sub-packages contains domestic classes for this Chronicle library.
 * Domestic classes shall <em>never</em> be used directly. Instead, domestic classes are sometimes
 * used internally by other Chronicle Libraries.
 * <p>
 *  Specifically, the following actions (including, but not limited to) are not allowed
 *  on domestic classes and packages:
 *  <ul>
 *      <li>Casting to</li>
 *      <li>Reflection of any kind</li>
 *      <li>Explicit Serialize/deserialize</li>
 *  </ul>
 * <p>
 * The classes in this package and any sub-package are subject to
 * changes at any time for any reason.
 */
package net.openhft.chronicle.queue.internal.domestic;
