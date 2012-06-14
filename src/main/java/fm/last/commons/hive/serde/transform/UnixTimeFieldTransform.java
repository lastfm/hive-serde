/*
 * Copyright 2012 Last.fm
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package fm.last.commons.hive.serde.transform;

import java.util.TimeZone;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import fm.last.commons.hive.serde.FieldTransform;

/**
 * {@link FieldTransform} implementation that can convert unixtime specified in <b>seconds</b> into a {@link String}
 * based representation of the format 'yyyy-MM-dd HH:mm:ss' and adjusted to the UTC timezone. The conversion will only
 * occur if the destination column type is {@link TypeInfoFactory#stringTypeInfo} - otherwise the input value is
 * returned.
 */
public class UnixTimeFieldTransform extends AbstractStringFieldTransform implements FieldTransform {

  private static final FastDateFormat UNIX_TIME_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss",
      TimeZone.getTimeZone("UTC"));

  /**
   * @param value {@link Integer} value representing a unixtime in seconds.
   */
  @Override
  public Object transform(Object value, TypeInfo columnType) {
    if (isColumnOfStringType(columnType)) {
      int seconds = ((Integer) value).intValue();
      long millis = seconds * 1000L;
      return UNIX_TIME_FORMAT.format(Long.valueOf(millis));
    }
    return value;
  }

}
