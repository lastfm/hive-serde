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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Default {@link Row} implementation.
 */
class ArrayListRow implements Row {

  private final TableDefinition tableDefinition;
  private final List<Object> row;
  private final List<TypeInfo> columnTypes;

  ArrayListRow(TableDefinition tableDefinition) {
    List<String> columnNames = tableDefinition.getColumnNames();
    columnTypes = tableDefinition.getColumnTypes();
    row = new ArrayList<Object>(columnNames.size());

    for (int i = 0; i < columnNames.size(); i++) {
      row.add(null);
    }

    this.tableDefinition = tableDefinition;
  }

  @Override
  public void setField(Field field, Object value) {
    Set<Integer> columnIds = tableDefinition.getColumnIdsForField(field);
    if (columnIds != null) {
      for (int columnId : columnIds) {
        FieldTransform transform = field.getTransform();
        TypeInfo columnType = columnTypes.get(columnId);
        row.set(columnId, transform.transform(value, columnType));
      }
    }
  }

  List<Object> asArrayList() {
    return row;
  }

}
