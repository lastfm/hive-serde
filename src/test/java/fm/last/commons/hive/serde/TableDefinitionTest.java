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

import static org.apache.hadoop.hive.serde.Constants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.INT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde.Constants.TINYINT_TYPE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class TableDefinitionTest {

  private static final String RECORD_ID = "record_id";
  private static final String USER_ID = "user_id";
  private static final String UNIX_TIME = "unix_time";
  private static final String VALUE_1 = "value_1";
  private static final String VALUE_2 = "value_2";
  private static final String VALUE_3 = "value_3";
  private static final String VALUE_4 = "value_4";
  private static final String VALUE_5 = "value_5";

  private static final List<String> COLUMN_NAMES = Lists.newArrayList(RECORD_ID, USER_ID, UNIX_TIME, VALUE_1, VALUE_2,
      VALUE_3, VALUE_4, VALUE_5);
  private static final List<String> COLUMN_TYPES = Lists.newArrayList(BIGINT_TYPE_NAME, INT_TYPE_NAME, INT_TYPE_NAME,
      INT_TYPE_NAME, INT_TYPE_NAME, TINYINT_TYPE_NAME, TINYINT_TYPE_NAME, INT_TYPE_NAME);

  private Properties table;
  private TableDefinition tableDefinition;

  @Before
  public void initialise() {
    table = new Properties();
  }

  @Test
  public void typical() {
    table.setProperty("columns", Joiner.on(',').join(COLUMN_NAMES));
    table.setProperty("columns.types", Joiner.on(',').join(COLUMN_TYPES));

    tableDefinition = new TableDefinition(table, TestField.Factory.INSTANCE);
    Set<Integer> columnIdsForField = tableDefinition.getColumnIdsForField(TestField.RECORD_ID);
    assertEquals(1, columnIdsForField.size());
    assertTrue(columnIdsForField.contains(0));
    columnIdsForField = tableDefinition.getColumnIdsForField(TestField.USER_ID);
    assertEquals(1, columnIdsForField.size());
    assertTrue(columnIdsForField.contains(1));
    columnIdsForField = tableDefinition.getColumnIdsForField(TestField.UNIX_TIME);
    assertEquals(1, columnIdsForField.size());
    assertTrue(columnIdsForField.contains(2));
    columnIdsForField = tableDefinition.getColumnIdsForField(TestField.VALUE_1);
    assertEquals(1, columnIdsForField.size());
    assertTrue(columnIdsForField.contains(3));
    columnIdsForField = tableDefinition.getColumnIdsForField(TestField.VALUE_2);
    assertEquals(1, columnIdsForField.size());
    assertTrue(columnIdsForField.contains(4));
    columnIdsForField = tableDefinition.getColumnIdsForField(TestField.VALUE_3);
    assertEquals(1, columnIdsForField.size());
    assertTrue(columnIdsForField.contains(5));
    columnIdsForField = tableDefinition.getColumnIdsForField(TestField.VALUE_4);
    assertEquals(1, columnIdsForField.size());
    assertTrue(columnIdsForField.contains(6));
    columnIdsForField = tableDefinition.getColumnIdsForField(TestField.VALUE_5);
    assertEquals(1, columnIdsForField.size());
    assertTrue(columnIdsForField.contains(7));
  }

  @Test
  public void subsetOfFieldsDeclaredAsTableColumns() {
    table.setProperty("columns", Joiner.on(',').join(UNIX_TIME, VALUE_3, VALUE_5));
    table.setProperty("columns.types", Joiner.on(',').join(INT_TYPE_NAME, TINYINT_TYPE_NAME, INT_TYPE_NAME));

    tableDefinition = new TableDefinition(table, TestField.Factory.INSTANCE);
    Set<Integer> columnIdsForField = tableDefinition.getColumnIdsForField(TestField.UNIX_TIME);
    assertEquals(1, columnIdsForField.size());
    assertTrue(columnIdsForField.contains(0));
    columnIdsForField = tableDefinition.getColumnIdsForField(TestField.VALUE_3);
    assertEquals(1, columnIdsForField.size());
    assertTrue(columnIdsForField.contains(1));
    columnIdsForField = tableDefinition.getColumnIdsForField(TestField.VALUE_5);
    assertEquals(1, columnIdsForField.size());
    assertTrue(columnIdsForField.contains(2));

    assertEquals(INT_TYPE_NAME, tableDefinition.getColumnTypes().get(0).getTypeName());
    assertEquals(TINYINT_TYPE_NAME, tableDefinition.getColumnTypes().get(1).getTypeName());
    assertEquals(INT_TYPE_NAME, tableDefinition.getColumnTypes().get(2).getTypeName());
  }

  @Test
  public void subsetOfFieldsWithSomeColumnNamesMapped() {
    table.setProperty("columns", Joiner.on(',').join("time", "operationType", VALUE_5));
    table.setProperty("columns.types", Joiner.on(',').join(INT_TYPE_NAME, TINYINT_TYPE_NAME, INT_TYPE_NAME));
    table.setProperty("time", TestField.UNIX_TIME.getFieldName());
    table.setProperty("operationType", TestField.VALUE_3.getFieldName());

    tableDefinition = new TableDefinition(table, TestField.Factory.INSTANCE);
    Set<Integer> columnIdsForField = tableDefinition.getColumnIdsForField(TestField.UNIX_TIME);
    assertEquals(1, columnIdsForField.size());
    assertTrue(columnIdsForField.contains(0));
    columnIdsForField = tableDefinition.getColumnIdsForField(TestField.VALUE_3);
    assertEquals(1, columnIdsForField.size());
    assertTrue(columnIdsForField.contains(1));
    columnIdsForField = tableDefinition.getColumnIdsForField(TestField.VALUE_5);
    assertEquals(1, columnIdsForField.size());
    assertTrue(columnIdsForField.contains(2));

    assertEquals(INT_TYPE_NAME, tableDefinition.getColumnTypes().get(0).getTypeName());
    assertEquals(TINYINT_TYPE_NAME, tableDefinition.getColumnTypes().get(1).getTypeName());
    assertEquals(INT_TYPE_NAME, tableDefinition.getColumnTypes().get(2).getTypeName());
  }

  @Test
  public void subsetOfFieldsWithSomeColumnNamesMappedMultipleTimes() {
    table.setProperty("columns", Joiner.on(',').join(UNIX_TIME, VALUE_1, VALUE_5, "resource"));
    table.setProperty("columns.types",
        Joiner.on(',').join(INT_TYPE_NAME, INT_TYPE_NAME, INT_TYPE_NAME, STRING_TYPE_NAME));
    table.setProperty("resource", TestField.VALUE_1.getFieldName());

    tableDefinition = new TableDefinition(table, TestField.Factory.INSTANCE);
    Set<Integer> columnIdsForField = tableDefinition.getColumnIdsForField(TestField.UNIX_TIME);
    assertEquals(1, columnIdsForField.size());
    assertTrue(columnIdsForField.contains(0));
    columnIdsForField = tableDefinition.getColumnIdsForField(TestField.VALUE_1);
    assertEquals(2, columnIdsForField.size());
    assertTrue(columnIdsForField.contains(1));
    assertTrue(columnIdsForField.contains(3));
    columnIdsForField = tableDefinition.getColumnIdsForField(TestField.VALUE_5);
    assertEquals(1, columnIdsForField.size());
    assertTrue(columnIdsForField.contains(2));

    assertEquals(INT_TYPE_NAME, tableDefinition.getColumnTypes().get(0).getTypeName());
    assertEquals(INT_TYPE_NAME, tableDefinition.getColumnTypes().get(1).getTypeName());
    assertEquals(INT_TYPE_NAME, tableDefinition.getColumnTypes().get(2).getTypeName());
    assertEquals(STRING_TYPE_NAME, tableDefinition.getColumnTypes().get(3).getTypeName());
  }

  @Test
  public void getColumnNames() {
    table.setProperty("columns", Joiner.on(',').join(COLUMN_NAMES));
    table.setProperty("columns.types", Joiner.on(',').join(COLUMN_TYPES));

    tableDefinition = new TableDefinition(table, TestField.Factory.INSTANCE);
    assertEquals(COLUMN_NAMES, tableDefinition.getColumnNames());
  }

  @Test
  public void getColumnTypes() {
    table.setProperty("columns", Joiner.on(',').join(COLUMN_NAMES));
    table.setProperty("columns.types", Joiner.on(',').join(COLUMN_TYPES));

    tableDefinition = new TableDefinition(table, TestField.Factory.INSTANCE);
    assertEquals(COLUMN_TYPES.size(), tableDefinition.getColumnTypes().size());

    assertEquals(BIGINT_TYPE_NAME, tableDefinition.getColumnTypes().get(0).getTypeName());
    assertEquals(INT_TYPE_NAME, tableDefinition.getColumnTypes().get(1).getTypeName());
    assertEquals(INT_TYPE_NAME, tableDefinition.getColumnTypes().get(2).getTypeName());
    assertEquals(INT_TYPE_NAME, tableDefinition.getColumnTypes().get(3).getTypeName());
    assertEquals(INT_TYPE_NAME, tableDefinition.getColumnTypes().get(4).getTypeName());
    assertEquals(TINYINT_TYPE_NAME, tableDefinition.getColumnTypes().get(5).getTypeName());
    assertEquals(TINYINT_TYPE_NAME, tableDefinition.getColumnTypes().get(6).getTypeName());
    assertEquals(INT_TYPE_NAME, tableDefinition.getColumnTypes().get(7).getTypeName());
  }

}
