/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.core.dag.actions;

import lombok.NonNull;

import java.util.Collections;
import java.util.List;

public class JavaQueueAction extends AbstractAction {

    private static final long serialVersionUID = -1646055642481336747L;

    public JavaQueueAction(int id, @NonNull String name, @NonNull List<Action> upstreams) {
        super(id, name, upstreams, Collections.emptyList());
    }

    public JavaQueueAction(int id, @NonNull String name) {
        super(id, name, Collections.emptyList());
    }
}
