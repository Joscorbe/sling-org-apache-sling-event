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
package org.apache.sling.event.impl.jobs.queues;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.PrintStream;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jcr.ItemExistsException;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.version.VersionException;

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.scheduler.Scheduler;
import org.apache.sling.commons.threads.ThreadPool;
import org.apache.sling.commons.threads.ThreadPoolManager;
import org.apache.sling.event.impl.jobs.JobConsumerManager;
import org.apache.sling.event.impl.jobs.JobManagerImpl;
import org.apache.sling.event.impl.jobs.config.InternalQueueConfiguration;
import org.apache.sling.event.impl.jobs.config.InternalQueueConfiguration.Config;
import org.apache.sling.event.impl.jobs.config.JobManagerConfiguration;
import org.apache.sling.event.impl.jobs.config.JobManagerConfigurationTestFactory;
import org.apache.sling.event.impl.jobs.config.QueueConfigurationManager;
import org.apache.sling.event.impl.jobs.config.QueueConfigurationManager.QueueInfo;
import org.apache.sling.event.impl.jobs.config.TopologyCapabilities;
import org.apache.sling.event.impl.jobs.jmx.QueuesMBeanImpl;
import org.apache.sling.event.impl.jobs.scheduling.JobSchedulerImpl;
import org.apache.sling.event.impl.jobs.stats.StatisticsManager;
import org.apache.sling.event.impl.support.Environment;
import org.apache.sling.event.impl.support.ResourceHelper;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.JobManager;
import org.apache.sling.event.jobs.Queue;
import org.apache.sling.event.jobs.QueueConfiguration.ThreadPriority;
import org.apache.sling.event.jobs.QueueConfiguration.Type;
import org.apache.sling.testing.mock.osgi.MapUtil;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.sling.MockSling;
import org.apache.sling.testing.mock.sling.ResourceResolverType;
import org.apache.sling.testing.mock.sling.builder.ContentBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;


@RunWith(MockitoJUnitRunner.class)
public class JobQueueMetricTest {

    @Mock
    private JobSchedulerImpl jobScheduler;

    @Mock
    private EventAdmin eventAdmin;

    @Mock
    private JobConsumerManager jobConsumerManager;

    @Mock
    private ThreadPoolManager threadPoolManager;

    @Mock
    private ThreadPool threadPool;

    @Mock
    private StatisticsManager statisticsManager;

    @Mock
    private QueueConfigurationManager queueConfigurationManager;

    @Mock
    private Scheduler scheduler;

    private static final String QUERY_ROOT = "/var/eventing/foobar";
    private static final JobManager.QueryType QUERY_TYPE = JobManager.QueryType.ACTIVE;

    private QueueInfo info;
    private TopologyCapabilities capabilities;
    private PrintStream originalErr;
    private ByteArrayOutputStream outContent;
    private JobManagerConfiguration configuration;

    private QueuesMBeanImpl queuesMBean;

    private String ownSlingId;

    private ResourceResolverFactory factory;

    private ComponentContext componentContext;

    private BundleContext bundleContext;

    /** object under test */
    private QueueManager queueManager;

    @Before
    public void setUp() throws Throwable {
        ownSlingId = UUID.randomUUID().toString();
        Environment.APPLICATION_ID = ownSlingId;
        componentContext = MockOsgi.newComponentContext();
        bundleContext = componentContext.getBundleContext();

        factory = MockSling.newResourceResolverFactory(ResourceResolverType.JCR_OAK, bundleContext);

        queuesMBean = new QueuesMBeanImpl();
        queuesMBean.activate(bundleContext);

        configuration = JobManagerConfigurationTestFactory.create(JobManagerConfiguration.DEFAULT_REPOSITORY_PATH,
                factory, queueConfigurationManager);
        JobManagerConfiguration configurationSpy = spy(configuration);

        info = new QueueInfo();
        info.queueConfiguration = mock(InternalQueueConfiguration.class);
        when(queueConfigurationManager.getQueueInfo(anyString())).thenReturn(info);
        doReturn(queueConfigurationManager).when(configurationSpy).getQueueConfigurationManager();

        queueManager = QueueManager.newForTest(eventAdmin, jobConsumerManager,
                queuesMBean, threadPoolManager, threadPool, configuration, statisticsManager);

        initQueueConfigurationManagerMocks();

        queueManager.activate(null);

        @SuppressWarnings("deprecation")
        ResourceResolver resourceResolver = factory.getAdministrativeResourceResolver(null);
        ContentBuilder contentBuilder = new ContentBuilder(resourceResolver);

        resourceResolver.commit();
    }

    private void initQueueConfigurationManagerMocks() {
        when(queueConfigurationManager.getQueueInfo(anyString())).thenAnswer(new Answer<QueueInfo>() {

            private final Map<String, QueueInfo> queueInfos = new HashMap<>();

            @Override
            public QueueInfo answer(InvocationOnMock invocation) throws Throwable {
                final String topic = (String) invocation.getArguments()[0];
                QueueInfo queueInfo = queueInfos.get(topic);
                if ( queueInfo == null ) {
                    queueInfo = createQueueInfo(topic);
                    queueInfos.put(topic, queueInfo);
                }
                return queueInfo;
            }

            private QueueInfo createQueueInfo(final String topic) {
                final QueueInfo result = new QueueInfo();
                result.queueName = "Queue for topic=" + topic;
                Map<String, Object> props = new HashMap<>();
                Config cconfig = Mockito.mock(Config.class);
                Mockito.when(cconfig.queue_priority()).thenReturn(ThreadPriority.NORM.name());
                Mockito.when(cconfig.queue_type()).thenReturn(Type.ORDERED.name());
                Mockito.when(cconfig.queue_maxparallel()).thenReturn(1.0);
                result.queueConfiguration = InternalQueueConfiguration.fromConfiguration(props, cconfig);
                result.targetId = ownSlingId;
                return result;
            }

        });
    }

    /*@Before
    public void init() throws IllegalAccessException {
        configuration = mock(JobManagerConfiguration.class);
        when(configuration.getUniquePath(any(), any(), any(), any())).thenReturn("/");
        when(configuration.getUniqueId(any())).thenReturn("1");
        when(configuration.getAuditLogger()).thenReturn(mock(Logger.class));

        QueueConfigurationManager queueConfigMgr = mock(QueueConfigurationManager.class);
        this.info = new QueueInfo();
        info.queueConfiguration = mock(InternalQueueConfiguration.class);
        when(queueConfigMgr.getQueueInfo(anyString())).thenReturn(info);
        when(configuration.getQueueConfigurationManager()).thenReturn(queueConfigMgr);

        this.capabilities = mock(TopologyCapabilities.class);
        when(configuration.getTopologyCapabilities()).thenReturn(capabilities);

        when(capabilities.detectTarget(eq("is/assigned"), any(), any())).thenReturn("assigned");

        ResourceResolver resolver = mock(ResourceResolver.class);
        when(resolver.getResource(anyString())).thenReturn(mock(Resource.class));
        when(configuration.createResourceResolver()).thenReturn(resolver);
    }*/

    @Test
    public void testNumberOfConfiguredQueuesMetric() throws Exception {
        StatisticsManager statisticsManagerLocal = new StatisticsManager();
        queueManager = QueueManager.newForTest(eventAdmin, jobConsumerManager,
                queuesMBean, threadPoolManager, threadPool, configuration, statisticsManagerLocal);

        initQueueConfigurationManagerMocks();

        queueManager.activate(null);

        @SuppressWarnings("deprecation")
        ResourceResolver resourceResolver = factory.getAdministrativeResourceResolver(null);
        ContentBuilder contentBuilder = new ContentBuilder(resourceResolver);

        JobManagerImpl jobManager = new JobManagerImpl();

        final String topic = "aTopic";
        int counter = 0;
        for (int year = 2019; year <= 2022; year++) {
            for (int month = 1; month <= 12; month++) {
                String jobId = year + "/" + month + "/1/20/0/" + ownSlingId + "_" + counter;
                jobManager.addJob(topic, MapUtil.toMap(ResourceHelper.PROPERTY_JOB_TOPIC, topic, ResourceHelper.PROPERTY_JOB_ID, jobId, Job.PROPERTY_JOB_CREATED, Calendar.getInstance()), Collections.EMPTY_LIST);
                counter++;
            }
        }
        resourceResolver.commit();

        assertEquals(0, statisticsManagerLocal.getGlobalStatistics().getNumberOfConfiguredQueues());



        queueManager.fullTopicScan();

        assertEquals(36, statisticsManagerLocal.getGlobalStatistics().getNumberOfConfiguredQueues());
    }

    /*private Resource createJob(ContentBuilder contentBuilder, String localSlingId, String topic, int year, int month) throws ItemExistsException, PathNotFoundException, VersionException, ConstraintViolationException, LockException, RepositoryException {
        // /var/eventing/jobs/assigned/<slingId>/<topic>/2020/10/13/19/26
        String applicationId = localSlingId;
        String counter = String.valueOf(jobCnt++);
        String jobId = year + "/" + month + "/1/20/0/" + applicationId + "_" + counter;
        String path = JobManagerConfiguration.DEFAULT_REPOSITORY_PATH + "/assigned/" + localSlingId + "/" + topic + "/" + jobId;

        final UnDeserializableDataObject uddao = new UnDeserializableDataObject();
        return contentBuilder.resource(path,
                ResourceHelper.PROPERTY_JOB_TOPIC, topic,
                ResourceHelper.PROPERTY_JOB_ID, jobId,
                Job.PROPERTY_JOB_CREATED, Calendar.getInstance(),
                "uddao", uddao);
    }*/

    private static final class UnDeserializableDataObject implements Externalizable {
        private static final long serialVersionUID = 1L;

        public UnDeserializableDataObject() {

        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.write(42);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        }
    }
}
