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
package fm.last.commons.hive.serde;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.junit.Ignore;

import com.google.common.collect.Maps;

import fm.last.commons.hive.serde.transform.IdentityFieldTransform;
import fm.last.commons.hive.serde.transform.UnixTimeFieldTransform;

@Ignore
enum TestField implements Field {
  /* */
  RECORD_ID("record_id"),
  /* */
  USER_ID("user_id"),
  /* */
  UNIX_TIME("unix_time", new UnixTimeFieldTransform()),
  /* */
  VALUE_1("value_1"),
  /* */
  VALUE_2("value_2"),
  /* */
  VALUE_3("value_3"),
  /* */
  VALUE_4("value_4"),
  /* */
  VALUE_5("value_5");

  private static final Map<String, TestField> nameToField;

  static {
    Map<String, TestField> map = Maps.newHashMap();
    for (TestField field : values()) {
      map.put(field.getFieldName(), field);
    }
    nameToField = Collections.unmodifiableMap(map);
  }

  private final String fieldName;
  private final FieldTransform transform;

  private TestField(String fieldName, FieldTransform transform) {
    this.fieldName = fieldName;
    this.transform = transform;
  }

  private TestField(String fieldName) {
    this(fieldName, IdentityFieldTransform.INSTANCE);
  }

  @Override
  public String getFieldName() {
    return fieldName;
  }

  @Override
  public FieldTransform getTransform() {
    return transform;
  }

  static enum Factory implements FieldFactory {
    INSTANCE;

    @Override
    public TestField valueOfFieldName(String fieldName) {
      return nameToField.get(fieldName);
    }

    @Override
    public Set<String> getFieldNames() {
      return nameToField.keySet();
    }
  }

}
