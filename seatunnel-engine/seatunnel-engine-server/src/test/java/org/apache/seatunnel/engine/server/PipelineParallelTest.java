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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.map.IMap;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;

public class PipelineParallelTest extends AbstractSeaTunnelServerTest {
    public static String CONF_PATH = "fakesource_to_console_pipeline_parallel.conf";
    public static String CONF_PATH2 = "fakesource_to_console_pipeline_parallel2.conf";

    @Test
    public void testPipelineParallelStart() {
        long jobId = System.currentTimeMillis();
        // ipeline_parallelism = 3
        // pipeline_wait_seconds = 7
        System.out.println("parallel jobId: " + jobId);
        startJob(jobId, CONF_PATH, false);

        Map<Integer, Long> scheduleTimeMap = new HashMap<>();
        Map<Integer, Long> finishTimeMap = new HashMap<>();
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            long count = finishTimeMap.size();
                            if (count < 9) {
                                IMap<Object, Long[]> map =
                                        nodeEngine
                                                .getHazelcastInstance()
                                                .getMap(Constant.IMAP_STATE_TIMESTAMPS);
                                if (map.get(jobId) != null) {
                                    for (int i = 1; i <= 10; i++) {
                                        PipelineLocation pipelineLocation =
                                                new PipelineLocation(jobId, i);
                                        long l =
                                                Optional.ofNullable(map.get(pipelineLocation))
                                                        .map(
                                                                a ->
                                                                        a[
                                                                                PipelineStatus
                                                                                        .SCHEDULED
                                                                                        .ordinal()])
                                                        .orElse(0L);
                                        if (l > 0 && scheduleTimeMap.getOrDefault(i, 0L) == 0) {
                                            scheduleTimeMap.put(i, l);
                                        }

                                        long l1 =
                                                Optional.ofNullable(map.get(pipelineLocation))
                                                        .map(
                                                                a ->
                                                                        a[
                                                                                PipelineStatus
                                                                                        .FINISHED
                                                                                        .ordinal()])
                                                        .orElse(0L);
                                        if (l1 > 0 && finishTimeMap.getOrDefault(i, 0L) == 0) {
                                            finishTimeMap.put(i, l1);
                                        }
                                    }
                                    count = finishTimeMap.size();
                                }
                            }
                            JobStatus jobStatus =
                                    server.getCoordinatorService()
                                            .getJobHistoryService()
                                            .getJobDetailState(jobId)
                                            .getJobStatus();
                            Assertions.assertEquals(JobStatus.FINISHED, jobStatus);
                            Assertions.assertTrue(count >= 9);

                            List<A> li = new ArrayList<>();
                            List<A> finalLi = li;
                            scheduleTimeMap.forEach(
                                    (a, b) -> {
                                        A aa = new A();
                                        aa.pipelineID = a;
                                        aa.scheduleTime = b;
                                        aa.finishTime = finishTimeMap.get(a);
                                        finalLi.add(aa);
                                    });
                            li =
                                    li.stream()
                                            .sorted(
                                                    (a, b) ->
                                                            Math.toIntExact(
                                                                    a.getScheduleTime()
                                                                            - b.getScheduleTime()))
                                            .collect(Collectors.toList());
                            System.out.println("pipeline schedule time and finish time: " + li);
                            for (int index = li.size() - 1; index > 3; index--) {
                                boolean b =
                                        li.get(index).getScheduleTime()
                                                > findMin(
                                                        li.get(index - 1).getFinishTime(),
                                                        li.get(index - 2).getFinishTime(),
                                                        li.get(index - 3).getFinishTime());
                                Assertions.assertTrue(b);
                            }
                        });
    }

    @Data
    static class A {
        Integer pipelineID;
        Long scheduleTime;
        Long finishTime;
    }

    public static long findMin(Long... l) {
        long l1 = l[0];
        for (int i = 1; i < l.length; i++) {
            if (l[i] < l1) {
                l1 = l[i];
            }
        }
        return l1;
    }

    @Test
    public void testPipelineParallelStart2() {
        long jobId = System.currentTimeMillis();
        // pipeline_parallelism = 4
        // pipeline_wait_seconds = 10
        startJob(jobId, CONF_PATH2, false);

        Map<Integer, Long> scheduleTimeMap = new HashMap<>();
        Map<Integer, Long> finishTimeMap = new HashMap<>();
        await().atMost(180000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            long count = finishTimeMap.size();
                            if (count < 9) {
                                IMap<Object, Long[]> map =
                                        nodeEngine
                                                .getHazelcastInstance()
                                                .getMap(Constant.IMAP_STATE_TIMESTAMPS);
                                if (map.get(jobId) != null) {
                                    for (int i = 1; i <= 10; i++) {
                                        PipelineLocation pipelineLocation =
                                                new PipelineLocation(jobId, i);
                                        long l =
                                                Optional.ofNullable(map.get(pipelineLocation))
                                                        .map(
                                                                a ->
                                                                        a[
                                                                                PipelineStatus
                                                                                        .SCHEDULED
                                                                                        .ordinal()])
                                                        .orElse(0L);
                                        if (l > 0 && scheduleTimeMap.getOrDefault(i, 0L) == 0) {
                                            scheduleTimeMap.put(i, l);
                                        }

                                        long l1 =
                                                Optional.ofNullable(map.get(pipelineLocation))
                                                        .map(
                                                                a ->
                                                                        a[
                                                                                PipelineStatus
                                                                                        .FINISHED
                                                                                        .ordinal()])
                                                        .orElse(0L);
                                        if (l1 > 0 && finishTimeMap.getOrDefault(i, 0L) == 0) {
                                            finishTimeMap.put(i, l1);
                                        }
                                    }
                                    count = finishTimeMap.size();
                                }
                            }
                            JobStatus jobStatus =
                                    server.getCoordinatorService()
                                            .getJobHistoryService()
                                            .getJobDetailState(jobId)
                                            .getJobStatus();
                            Assertions.assertEquals(JobStatus.FINISHED, jobStatus);
                            Assertions.assertTrue(count >= 9);

                            List<A> li = new ArrayList<>();
                            List<A> finalLi = li;
                            scheduleTimeMap.forEach(
                                    (a, b) -> {
                                        A aa = new A();
                                        aa.pipelineID = a;
                                        aa.scheduleTime = b;
                                        aa.finishTime = finishTimeMap.get(a);
                                        finalLi.add(aa);
                                    });
                            System.out.println("pipeline schedule time and finish time: " + li);

                            li =
                                    li.stream()
                                            .sorted(
                                                    (a, b) ->
                                                            Math.toIntExact(
                                                                    a.getScheduleTime()
                                                                            - b.getScheduleTime()))
                                            .collect(Collectors.toList());
                            for (int index = li.size() - 1; index > 4; index--) {
                                boolean b =
                                        li.get(index).getScheduleTime()
                                                > findMin(
                                                        li.get(index - 1).getFinishTime(),
                                                        li.get(index - 2).getFinishTime(),
                                                        li.get(index - 3).getFinishTime(),
                                                        li.get(index - 4).getFinishTime());
                                Assertions.assertTrue(b);
                            }
                        });
    }
}
