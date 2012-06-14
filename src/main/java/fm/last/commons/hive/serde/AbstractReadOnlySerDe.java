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

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

/**
 * Template SerDe implementation for read-only mapping of records to a Hive table. Typically you should extend this
 * class and implement {@link #mapRecordIntoRow(Writable, Row)}.
 * 
 * @param <R> The record type backing the Hive table.
 */
public abstract class AbstractReadOnlySerDe<R extends Writable> implements SerDe {

  private TableDefinition tableDefinition;
  private StructObjectInspector rowObjectInspector;
  private ArrayListRow row;
  private final Class<R> serializedClass;
  private final FieldFactory fieldFactory;

  public AbstractReadOnlySerDe(Class<R> serializedClass, FieldFactory fieldFactory) {
    this.serializedClass = serializedClass;
    this.fieldFactory = fieldFactory;
  }

  /**
   * Implementations should take the provided record {@link Writable} and map this into the {@link Row} provided. This
   * is generally achieved by calling {@link Row#setField(Field, Object)} for each field in the record.
   */
  abstract public void mapRecordIntoRow(R writable, Row row) throws SerDeException;

  @SuppressWarnings("unchecked")
  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    R record = (R) writable;
    mapRecordIntoRow(record, row);
    return row.asArrayList();
  }

  @Override
  public void initialize(Configuration conf, Properties table) throws SerDeException {
    tableDefinition = new TableDefinition(table, fieldFactory);
    List<TypeInfo> columnTypes = tableDefinition.getColumnTypes();
    List<String> columnNames = tableDefinition.getColumnNames();
    StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    rowObjectInspector = (StructObjectInspector) TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
    row = new ArrayListRow(tableDefinition);
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return serializedClass;
  }

  /**
   * Serialization not supported.
   * 
   * @throws UnsupportedOperationException always.
   */
  @Override
  public Writable serialize(Object arg0, ObjectInspector arg1) throws SerDeException {
    throw new UnsupportedOperationException(getClass().getSimpleName() + " does not support writes.");
  }

}
