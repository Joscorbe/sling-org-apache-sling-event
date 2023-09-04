/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sling.event.impl.jobs.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.scheduler.Scheduler;
import org.apache.sling.commons.threads.ThreadPool;
import org.apache.sling.commons.threads.ThreadPoolManager;
import org.apache.sling.event.impl.jobs.JobConsumerManager;
import org.apache.sling.event.impl.jobs.config.JobManagerConfiguration;
import org.apache.sling.event.impl.jobs.config.JobManagerConfigurationTestFactory;
import org.apache.sling.event.impl.jobs.config.QueueConfigurationManager;
import org.apache.sling.event.impl.jobs.jmx.QueuesMBeanImpl;
import org.apache.sling.event.impl.jobs.queues.QueueJobCache;
import org.apache.sling.event.impl.jobs.queues.QueueManager;
import org.apache.sling.event.impl.support.Environment;
import org.apache.sling.event.jobs.QueueConfiguration;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.sling.MockSling;
import org.apache.sling.testing.mock.sling.ResourceResolverType;
import org.apache.sling.testing.mock.sling.builder.ContentBuilder;
import org.junit.Test;
import org.mockito.Mock;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.event.EventAdmin;

import java.time.Clock;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class StatisticsImplTest {

    @Test public void testInitNoStartTime() {
        final long now = System.currentTimeMillis();
        final StatisticsImpl s = new StatisticsImpl();
        assertTrue(s.getStartTime() >= now);
        assertTrue(s.getStartTime() <= System.currentTimeMillis());
        assertEquals(0, s.getNumberOfFinishedJobs());
        assertEquals(0, s.getNumberOfCancelledJobs());
        assertEquals(0, s.getNumberOfFailedJobs());
        assertEquals(0, s.getNumberOfProcessedJobs());
        assertEquals(0, s.getNumberOfActiveJobs());
        assertEquals(0, s.getNumberOfQueuedJobs());
        assertEquals(0, s.getNumberOfJobs());
        assertEquals(-1, s.getLastActivatedJobTime());
        assertEquals(-1, s.getLastFinishedJobTime());
        assertEquals(0, s.getAverageWaitingTime());
        assertEquals(0, s.getAverageProcessingTime());
        assertTrue(s.getIdleTime() < 100);
        assertEquals(0, s.getActiveJobRunningTime());
    }

    @Test public void testInitStartTime() {
        final StatisticsImpl s = new StatisticsImpl(7000);
        assertEquals(7000L, s.getStartTime());
        assertEquals(0, s.getNumberOfFinishedJobs());
        assertEquals(0, s.getNumberOfCancelledJobs());
        assertEquals(0, s.getNumberOfFailedJobs());
        assertEquals(0, s.getNumberOfProcessedJobs());
        assertEquals(0, s.getNumberOfActiveJobs());
        assertEquals(0, s.getNumberOfQueuedJobs());
        assertEquals(0, s.getNumberOfJobs());
        assertEquals(-1, s.getLastActivatedJobTime());
        assertEquals(-1, s.getLastFinishedJobTime());
        assertEquals(0, s.getAverageWaitingTime());
        assertEquals(0, s.getAverageProcessingTime());
        assertEquals(System.currentTimeMillis() - 7000, s.getIdleTime());
    }

    @Test public void reset() {
        final StatisticsImpl s = new StatisticsImpl(7000);
        final long now = System.currentTimeMillis();
        s.reset();
        assertTrue(s.getStartTime() >= now);
        assertTrue(s.getStartTime() <= System.currentTimeMillis());
        assertEquals(0, s.getNumberOfFinishedJobs());
        assertEquals(0, s.getNumberOfCancelledJobs());
        assertEquals(0, s.getNumberOfFailedJobs());
        assertEquals(0, s.getNumberOfProcessedJobs());
        assertEquals(0, s.getNumberOfActiveJobs());
        assertEquals(0, s.getNumberOfQueuedJobs());
        assertEquals(0, s.getNumberOfJobs());
        assertEquals(-1, s.getLastActivatedJobTime());
        assertEquals(-1, s.getLastFinishedJobTime());
        assertEquals(0, s.getAverageWaitingTime());
        assertEquals(0, s.getAverageProcessingTime());
        assertTrue(s.getIdleTime() < 100);
        assertEquals(0, s.getActiveJobRunningTime());
    }

    @Test public void testJobFinished() {
        final StatisticsImpl s = new StatisticsImpl();

        final long now = System.currentTimeMillis();

        s.incQueued();
        s.incQueued();
        assertEquals(2L, s.getNumberOfQueuedJobs());
        assertEquals(0L, s.getNumberOfActiveJobs());
        assertEquals(2L, s.getNumberOfJobs());

        s.addActive(500);
        s.addActive(700);
        assertEquals(0L, s.getNumberOfQueuedJobs());
        assertEquals(2L, s.getNumberOfActiveJobs());
        assertEquals(2L, s.getNumberOfJobs());

        s.finishedJob(300);
        s.finishedJob(500);

        assertEquals(0L, s.getNumberOfActiveJobs());
        assertEquals(0L, s.getNumberOfQueuedJobs());
        assertEquals(2L, s.getNumberOfFinishedJobs());
        assertEquals(2L, s.getNumberOfProcessedJobs());
        assertEquals(0L, s.getNumberOfJobs());

        assertEquals(400L, s.getAverageProcessingTime());
        assertEquals(600L, s.getAverageWaitingTime());
        assertTrue(s.getLastFinishedJobTime() >= now);
        assertTrue(s.getLastFinishedJobTime() <= System.currentTimeMillis());
        assertTrue(s.getLastActivatedJobTime() >= now);
        assertTrue(s.getLastActivatedJobTime() <= System.currentTimeMillis());
    }

    @Test public void testJobFailed() {
        final StatisticsImpl s = new StatisticsImpl();

        final long now = System.currentTimeMillis();

        s.incQueued();
        s.incQueued();
        assertEquals(2L, s.getNumberOfQueuedJobs());
        assertEquals(0L, s.getNumberOfActiveJobs());
        assertEquals(2L, s.getNumberOfJobs());

        s.addActive(500);
        s.addActive(700);
        assertEquals(0L, s.getNumberOfQueuedJobs());
        assertEquals(2L, s.getNumberOfActiveJobs());
        assertEquals(2L, s.getNumberOfJobs());

        s.failedJob();
        s.failedJob();

        assertEquals(0L, s.getNumberOfActiveJobs());
        assertEquals(0L, s.getNumberOfQueuedJobs());
        assertEquals(0L, s.getNumberOfFinishedJobs());
        assertEquals(2L, s.getNumberOfProcessedJobs());
        assertEquals(2L, s.getNumberOfFailedJobs());
        assertEquals(0L, s.getNumberOfJobs());

        assertEquals(0, s.getAverageProcessingTime());
        assertEquals(600L, s.getAverageWaitingTime());
        assertEquals(-1L, s.getLastFinishedJobTime());
        assertTrue(s.getLastActivatedJobTime() >= now);
        assertTrue(s.getLastActivatedJobTime() <= System.currentTimeMillis());
    }

    @Test public void testJobCancelled() {
        final StatisticsImpl s = new StatisticsImpl();

        final long now = System.currentTimeMillis();

        s.incQueued();
        s.incQueued();
        assertEquals(2L, s.getNumberOfQueuedJobs());
        assertEquals(0L, s.getNumberOfActiveJobs());
        assertEquals(2L, s.getNumberOfJobs());

        s.addActive(500);
        s.addActive(700);
        assertEquals(0L, s.getNumberOfQueuedJobs());
        assertEquals(2L, s.getNumberOfActiveJobs());
        assertEquals(2L, s.getNumberOfJobs());

        s.cancelledJob();
        s.cancelledJob();

        assertEquals(0L, s.getNumberOfActiveJobs());
        assertEquals(0L, s.getNumberOfQueuedJobs());
        assertEquals(0L, s.getNumberOfFinishedJobs());
        assertEquals(2L, s.getNumberOfProcessedJobs());
        assertEquals(2L, s.getNumberOfCancelledJobs());
        assertEquals(0L, s.getNumberOfJobs());

        assertEquals(0, s.getAverageProcessingTime());
        assertEquals(600L, s.getAverageWaitingTime());
        assertEquals(-1L, s.getLastFinishedJobTime());
        assertTrue(s.getLastActivatedJobTime() >= now);
        assertTrue(s.getLastActivatedJobTime() <= System.currentTimeMillis());
    }

    @Test public void testIdleTimeWithoutJobExecution() {
        //Clock mockClock = mock(Clock.class);
        //when(mockClock.millis()).thenReturn(1690000000000L);

        final StatisticsTestImpl s = new StatisticsTestImpl(1690000000000L);
        assertEquals(0L, s.getIdleTime());

        //when(mockClock.millis()).thenReturn(1690000000500L);
        s.setFakeTimeMillis(1690000000500L);
        assertEquals(500L, s.getIdleTime());

        //when(mockClock.millis()).thenReturn(1690000001500L);
        s.setFakeTimeMillis(1690000001500L);
        assertEquals(1500L, s.getIdleTime());
    }

    @Test public void testIdleTimeDuringActiveJob() {
        //Clock mockClock = mock(Clock.class);
        //when(mockClock.millis()).thenReturn(1690000000000L);

        final StatisticsTestImpl s = new StatisticsTestImpl(1690000000000L);

        //when(mockClock.millis()).thenReturn(1690000001000L);
        s.setFakeTimeMillis(1690000001000L);
        assertEquals(1000L, s.getIdleTime());

        // Job starts after 1s in queue
        s.addActive(1000);
        assertEquals(0L, s.getIdleTime()); // Not idle, as job is running

        //when(mockClock.millis()).thenReturn(1690000002000L);
        s.setFakeTimeMillis(1690000002000L);
        assertEquals(0L, s.getIdleTime());  // Not idle while a job is running

        //when(mockClock.millis()).thenReturn(1690000005000L);
        s.setFakeTimeMillis(1690000005000L);

        // Job finishes after 3 seconds of execution
        s.finishedJob(3000);
        assertEquals(0L, s.getIdleTime());  // Not idle just after finishing a job

        //when(mockClock.millis()).thenReturn(1690000015000L);
        s.setFakeTimeMillis(1690000015000L);
        assertEquals(10000L, s.getIdleTime());
    }

    @Test public void testActiveJobRunningTime() {
        //Clock mockClock = mock(Clock.class);
        //when(mockClock.millis()).thenReturn(1690000000000L);

        final StatisticsTestImpl s = new StatisticsTestImpl(1690000000000L);

        //when(mockClock.millis()).thenReturn(1690000001000L);
        s.setFakeTimeMillis(1690000001000L);

        s.addActive(1000);
        assertEquals(0L, s.getActiveJobRunningTime());

        //when(mockClock.millis()).thenReturn(1690000002000L);
        s.setFakeTimeMillis(1690000002000L);
        assertEquals(1000L, s.getActiveJobRunningTime());

        //when(mockClock.millis()).thenReturn(1690000005000L);
        s.setFakeTimeMillis(1690000005000L);
        assertEquals(4000L, s.getActiveJobRunningTime());

        //when(mockClock.millis()).thenReturn(1690000006000L);
        s.setFakeTimeMillis(1690000006000L);
        s.finishedJob(5000);
        assertEquals(0L, s.getActiveJobRunningTime());
    }

    @Test public void testNumberOfTopicsPerQueue() {
        ResourceResolver mockResolver = mock(ResourceResolver.class);
        JobManagerConfiguration mockConfiguration = mock(JobManagerConfiguration.class);
        StatisticsManager mockStatisticsManager = mock(StatisticsManager.class);

        when(mockConfiguration.createResourceResolver()).thenReturn(mockResolver);
        when(mockResolver.getResource(anyString())).thenReturn(null);

        String queueName = "testQueue";
        Set<String> topics = new HashSet<>(Arrays.asList("topic1", "topic2", "topic3"));
        new QueueJobCache(mockConfiguration, queueName, mockStatisticsManager, QueueConfiguration.Type.UNORDERED, topics);

        verify(mockStatisticsManager, times(1)).topicsAssignedToQueue(eq(queueName), eq(topics));
    }

    @Test public void testTopicCountMetric() {
        final StatisticsImpl s = new StatisticsImpl();

        Set<String> topics = new HashSet<>(Arrays.asList("topic1", "topic2", "topic3"));
        s.setTopics(topics);

        assertEquals(3, s.getNumberOfTopics());
    }

    @Test public void testNumberOfConfiguredQueues() {
        final StatisticsImpl s = new StatisticsImpl();

        s.setNumberOfConfiguredQueues(5);
        assertEquals(5, s.getNumberOfConfiguredQueues());
    }

    private class StatisticsTestImpl extends StatisticsImpl {

        long fakeTimeMillis = 0;

        public StatisticsTestImpl(long startTime) {
            super(startTime);
            fakeTimeMillis = startTime;
        }

        @Override
        protected long getCurrentTimeMillis() {
            return fakeTimeMillis;
        }

        public void setFakeTimeMillis(long fakeTimeMillis) {
            this.fakeTimeMillis = fakeTimeMillis;
        }
    }
}
