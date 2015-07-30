/**
 * Copyright 2014 NAVER Corp.
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
package com.navercorp.pinpoint.bootstrap.plugin.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.navercorp.pinpoint.bootstrap.instrument.InstrumentableMethod;
import com.navercorp.pinpoint.bootstrap.interceptor.MethodDescriptor;

/**
 * Indicates that the target have to be cached. 
 * 
 * For now, only {@link MethodDescriptor} can be cached. 
 * You can also annotate {@link InstrumentableMethod} with this annotation 
 * but it makes the {@link MethodDescriptor} returned by {@link InstrumentableMethod#getDescriptor()} cached 
 * not {@link InstrumentableMethod} itself.
 * 
 * @author Jongho Moon
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface NoCache {
}
