/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.configcenter.support.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.configcenter.ConfigChangeEvent;
import org.apache.dubbo.configcenter.ConfigurationListener;
import org.apache.dubbo.configcenter.DynamicConfiguration;
import org.apache.dubbo.configcenter.DynamicConfigurationFactory;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * TODO refactor using mockito
 */
public class ZookeeperDynamicConfigurationTest {
    private static CuratorFramework client;

//    private static String zookeeperIp = "localhost";
    private static String zookeeperIp = "192.168.18.214";
    private static URL configUrl;
    private static int zkServerPort = 2181;
    private static TestingServer zkServer;
    private static DynamicConfiguration configuration;
    private static String configCenterNamespace = "dev";

    @BeforeAll
    public static void setUp() throws Exception {
        zkServer = new TestingServer(zkServerPort, true);

        client = CuratorFrameworkFactory.newClient(zookeeperIp + ":" + zkServerPort, 60 * 1000, 60 * 1000,
                new ExponentialBackoffRetry(1000, 3));
        client.start();

        try {
            setData("/" + configCenterNamespace + "/config/dubbo/dubbo.properties", "The content from dubbo.properties");
            setData("/" + configCenterNamespace + "/config/group*service:version/configurators", "The content from configurators");
            setData("/" + configCenterNamespace + "/config/appname", "The content from higer level node");
            setData("/" + configCenterNamespace + "/config/appname/tagrouters", "The content from appname tagrouters");
            setData("/" + configCenterNamespace + "/config/never.change.DemoService/configurators", "Never change value from configurators");
            setData("/" + configCenterNamespace + "/config/appname/condition", "app condition route");
        } catch (Exception e) {
            e.printStackTrace();
        }

        configUrl = URL.valueOf("zookeeper://" + zookeeperIp + ":" + zkServerPort + "/ConfigCenterConfig?namespace=" + configCenterNamespace);

        configuration = ExtensionLoader
                .getExtensionLoader(DynamicConfigurationFactory.class)
                .getExtension(configUrl.getProtocol())
                .getDynamicConfiguration(configUrl);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        zkServer.stop();
    }

    private static void setData(String path, String data) throws Exception {
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().forPath(path);
        }
        client.setData().forPath(path, data.getBytes());
    }

    @Test
    public void testGetConfig() throws Exception {
        Assertions.assertEquals("Never change value from configurators", configuration.getConfig("never.change.DemoService.configurators"));
        Assertions.assertEquals("The content from dubbo.properties", configuration.getConfigs("dubbo.properties", "dubbo"));
    }

    @Test
    public void testAddListener() throws Exception {
        CountDownLatch latch = new CountDownLatch(6);
        TestListener listener1 = new TestListener(latch);
        TestListener listener2 = new TestListener(latch);
        TestListener listener3 = new TestListener(latch);
        TestListener listener4 = new TestListener(latch);
        TestListener listener5 = new TestListener(latch);
        TestListener listener6 = new TestListener(latch);
        configuration.addListener("group*service:version.configurators", listener1);
        configuration.addListener("group*service:version.configurators", listener2);
        configuration.addListener("appname.tagrouters", listener3);
        configuration.addListener("appname.tagrouters", listener4);
        configuration.addListener("appname.condition", listener5);
        configuration.addListener("appname.condition", listener6);

        setData("/" + configCenterNamespace + "/config/group*service:version/configurators", "new value1");
        Thread.sleep(100);
        setData("/" + configCenterNamespace + "/config/appname/tagrouters", "new value2");
        Thread.sleep(100);
        setData("/" + configCenterNamespace + "/config/appname", "new value3");
        Thread.sleep(100);
        setData("/" + configCenterNamespace + "/config/appname/condition", "new value4");

        System.out.println("set data complete ......");
        Thread.sleep(5000);

        latch.await();
        Assertions.assertEquals(1, listener1.getCount("group*service:version.configurators"));
        Assertions.assertEquals(1, listener2.getCount("group*service:version.configurators"));
        Assertions.assertEquals(1, listener3.getCount("appname.tagrouters"));
        Assertions.assertEquals(1, listener4.getCount("appname.tagrouters"));
        Assertions.assertEquals(1, listener5.getCount("appname.condition"));
        Assertions.assertEquals(1, listener6.getCount("appname.condition"));

        Assertions.assertEquals("new value1", listener1.getValue());
        Assertions.assertEquals("new value1", listener2.getValue());
        Assertions.assertEquals("new value2", listener3.getValue());
        Assertions.assertEquals("new value2", listener4.getValue());
        Assertions.assertEquals("new value4", listener5.getValue());
        Assertions.assertEquals("new value4", listener6.getValue());
    }

    private class TestListener implements ConfigurationListener {
        private CountDownLatch latch;
        private String value;
        private Map<String, Integer> countMap = new HashMap<>();

        public TestListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void process(ConfigChangeEvent event) {
            System.out.println(this + ": " + event);
            Integer count = countMap.computeIfAbsent(event.getKey(), k -> new Integer(0));
            countMap.put(event.getKey(), ++count);

            value = event.getValue();
            latch.countDown();
        }

        public int getCount(String key) {
            return countMap.get(key);
        }

        public String getValue() {
            return value;
        }
    }

}
