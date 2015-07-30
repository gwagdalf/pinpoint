/*
 * Copyright 2015 NAVER Corp.
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

package com.navercorp.pinpoint.plugin.thrift.interceptor.tprotocol.client;

import static com.navercorp.pinpoint.plugin.thrift.ThriftScope.THRIFT_CLIENT_SCOPE;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import com.navercorp.pinpoint.bootstrap.interceptor.SimpleAroundInterceptor;
import com.navercorp.pinpoint.bootstrap.interceptor.group.ExecutionPolicy;
import com.navercorp.pinpoint.bootstrap.interceptor.group.InterceptorGroup;
import com.navercorp.pinpoint.bootstrap.interceptor.group.InterceptorGroupInvocation;
import com.navercorp.pinpoint.bootstrap.logging.PLogger;
import com.navercorp.pinpoint.bootstrap.logging.PLoggerFactory;
import com.navercorp.pinpoint.bootstrap.plugin.annotation.Group;
import com.navercorp.pinpoint.bootstrap.plugin.annotation.Name;
import com.navercorp.pinpoint.plugin.thrift.ThriftConstants;
import com.navercorp.pinpoint.plugin.thrift.ThriftRequestProperty;
import com.navercorp.pinpoint.plugin.thrift.ThriftHeader;

/**
 * This interceptor writes the trace data directly on the wire to allow remote tracing.
 * Trace data is written AFTER all the data fields of the Thrift message have been written out.
 * <p>
 * <tt>TServiceClientSendBaseInterceptor</tt> -> <b><tt>TProtocolWriteFieldStopInterceptor</tt></b>
 * <p>
 * Based on Thrift 0.8.0+
 * 
 * @author HyunGil Jeong
 * 
 * @see com.navercorp.pinpoint.plugin.thrift.interceptor.client.TServiceClientSendBaseInterceptor TServiceClientSendBaseInterceptor
 */
@Group(value=THRIFT_CLIENT_SCOPE, executionPolicy=ExecutionPolicy.INTERNAL)
public class TProtocolWriteFieldStopInterceptor implements SimpleAroundInterceptor, ThriftConstants {
    
    private final PLogger logger = PLoggerFactory.getLogger(this.getClass());
    private final boolean isDebug = logger.isDebugEnabled();

    private final InterceptorGroup group;
    
    public TProtocolWriteFieldStopInterceptor(
            @Name(THRIFT_CLIENT_SCOPE) InterceptorGroup group) {
        this.group = group;
    }
    
    @Override
    public void before(Object target, Object[] args) {
        if (isDebug) {
            logger.beforeInterceptor(target, args);
        }
        if (target instanceof TProtocol) {
            TProtocol oprot = (TProtocol)target;
            try {
                appendParentTraceInfo(oprot);
            } catch (Throwable t) {
                logger.warn("problem writing trace info", t);
            }
        }
    }

    @Override
    public void after(Object target, Object[] args, Object result, Throwable throwable) {
        // Do nothing
    }
    
    private void appendParentTraceInfo(TProtocol oprot) throws TException {
        InterceptorGroupInvocation currentTransaction = this.group.getCurrentInvocation();
        ThriftRequestProperty parentTraceInfo = (ThriftRequestProperty)currentTransaction.getAttachment();
        if (parentTraceInfo == null) {
            return;
        }
        boolean shouldSample = parentTraceInfo.shouldSample(true);
        if (!shouldSample) {
            parentTraceInfo.writeTraceHeader(ThriftHeader.THRFIT_SAMPLED, oprot);
            return;
        }
        parentTraceInfo.writeTraceHeader(ThriftHeader.THRIFT_TRACE_ID, oprot);
        parentTraceInfo.writeTraceHeader(ThriftHeader.THRIFT_SPAN_ID, oprot);
        parentTraceInfo.writeTraceHeader(ThriftHeader.THRIFT_PARENT_SPAN_ID, oprot);
        parentTraceInfo.writeTraceHeader(ThriftHeader.THRIFT_FLAGS, oprot);
        parentTraceInfo.writeTraceHeader(ThriftHeader.THRIFT_PARENT_APPLICATION_NAME, oprot);
        parentTraceInfo.writeTraceHeader(ThriftHeader.THRIFT_PARENT_APPLICATION_TYPE, oprot);
        parentTraceInfo.writeTraceHeader(ThriftHeader.THRIFT_HOST, oprot);
    }

}
