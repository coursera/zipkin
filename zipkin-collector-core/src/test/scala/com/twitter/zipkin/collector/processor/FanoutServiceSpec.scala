/*
 * Copyright 2012 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.twitter.zipkin.collector.processor

import com.twitter.finagle.Service
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers._
import org.scalatest.mock.MockitoSugar._
import org.mockito.Mockito.{never, times, verify, when}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.twitter.util.Future

@RunWith(classOf[JUnitRunner])
class FanoutServiceSpec extends WordSpec {
  "FanoutService" should {
    "fanout" in {
      val serv1 = mock[Service[Int, Unit]]
      val serv2 = mock[Service[Int, Unit]]

      val fanout = new FanoutService[Int](Seq(serv1, serv2))
      val item = 1

      when(serv1.apply(item)).thenReturn(Future.Unit)
      when(serv2.apply(item)).thenReturn(Future.Unit)

      fanout.apply(item)
      verify(serv1, times(1)).apply(item)
      verify(serv2, times(1)).apply(item)
    }
  }
}
