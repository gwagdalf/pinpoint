/*
 * Copyright 2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.pinpoint.common.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.navercorp.pinpoint.common.util.PropertyUtils;


/**
 * @author emeroad
 * @author minwoo.jung
 */
public class HbaseTemplate2IT {
    private static Connection connection;
    private static HbaseTemplate2 hbaseTemplate2;

    @BeforeClass
    public static void beforeClass() throws IOException {
        Properties properties = PropertyUtils.loadPropertyFromClassPath("test-hbase.properties");

        Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", properties.getProperty("hbase.client.host"));
        cfg.set("hbase.zookeeper.property.clientPort", properties.getProperty("hbase.client.port"));
        cfg.set("zookeeper.znode.parent", properties.getProperty("hbase.zookeeper.znode.parent"));


        cfg.set("hbase.rpc.timeout",properties.getProperty("hbase.rpc.timeout"));
        cfg.set("hbase.client.operation.timeout",properties.getProperty("hbase.client.operation.timeout"));
        cfg.set("hbase.ipc.client.socket.timeout.read",properties.getProperty("hbase.ipc.client.socket.timeout.read"));
        cfg.set("hbase.ipc.client.socket.timeout.write",properties.getProperty("hbase.ipc.client.socket.timeout.write"));
        cfg.set("zookeeper.session.timeout","3000");



        connection = ConnectionFactory.createConnection(cfg);
        Admin admin = connection.getAdmin();
        hbaseTemplate2 = new HbaseTemplate2();
        hbaseTemplate2.setConfiguration(cfg);
        hbaseTemplate2.setTableFactory(new HbaseTableFactory(connection));
        hbaseTemplate2.afterPropertiesSet();

//        try {
//            HBaseAdmin.checkHBaseAvailable(cfg);
//        } catch (ServiceException e) {
//            Assert.fail("unexpected exception :" + e.getCause());
//        }

    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (hbaseTemplate2 != null) {
            hbaseTemplate2.destroy();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @Ignore
    public void notExist() throws Exception {
        try {
            hbaseTemplate2.put(TableName.valueOf("NOT_EXIST"), new byte[] {0, 0, 0}, "familyName".getBytes(), "columnName".getBytes(), new byte[]{0, 0, 0});
            Assert.fail("exceptions");
        } catch (HbaseSystemException e) {
            RetriesExhaustedWithDetailsException exception = (RetriesExhaustedWithDetailsException)(e.getCause());
            if (!(exception.getCause(0) instanceof TableNotFoundException)) {
                Assert.fail("unexpected exception :" + e.getCause()); 
            }
        }

    }
}
