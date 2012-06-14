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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 * Represents defines a Hive table, encapsulating the column order, names, types and maps {@link Field}s to the
 * appropriate column(s).
 */
class TableDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(TableDefinition.class);

  private List<String> columnNames;
  private List<TypeInfo> columnTypes;
  private final boolean[] columnSortOrderIsDesc;
  private final FieldFactory fieldFactory;
  private final Multimap<Field, Integer> fieldNameToColumnId;

  /**
   * Creates a new {@link TableDefinition}.
   * 
   * @param table {@link Properties} declared on the Hive table.
   * @param fieldFactory A factory instance that can provide {@link Field} definitions to map to the Hive table columns.
   */
  TableDefinition(Properties table, FieldFactory fieldFactory) {
    this.fieldFactory = fieldFactory;
    String columnNameProperty = table.getProperty("columns");
    String columnTypeProperty = table.getProperty("columns.types");

    if (columnNameProperty.length() == 0) {
      columnNames = Collections.emptyList();
    } else {
      columnNames = Collections.unmodifiableList(Arrays.asList(columnNameProperty.split(",")));
    }
    if (columnTypeProperty.length() == 0) {
      columnTypes = Collections.emptyList();
    } else {
      columnTypes = Collections.unmodifiableList(TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty));
    }
    if (columnNames.size() != columnTypes.size()) {
      throw new IllegalStateException("columnNames.size() != columnTypes.size()");
    }

    String columnSortOrder = table.getProperty(Constants.SERIALIZATION_SORT_ORDER);
    columnSortOrderIsDesc = new boolean[columnNames.size()];
    for (int i = 0; i < columnSortOrderIsDesc.length; i++) {
      columnSortOrderIsDesc[i] = (columnSortOrder != null && columnSortOrder.charAt(i) == '-');
    }

    fieldNameToColumnId = Multimaps.newMultimap(new HashMap<Field, Collection<Integer>>(),
        new Supplier<Set<Integer>>() {
          @Override
          public Set<Integer> get() {
            return new HashSet<Integer>();
          }
        });
    mapColumnsToFields(table);
  }

  Set<Integer> getColumnIdsForField(Field field) {
    return (Set<Integer>) fieldNameToColumnId.get(field);
  }

  List<String> getColumnNames() {
    return columnNames;
  }

  List<TypeInfo> getColumnTypes() {
    return columnTypes;
  }

  private void mapColumnsToFields(Properties table) {
    for (int columnId = 0; columnId < columnNames.size(); columnId++) {
      String columnName = columnNames.get(columnId);
      Field field = mapColumnToField(table, columnName);
      if (field != null) {
        fieldNameToColumnId.put(field, columnId);
      }
    }
  }

  private Field mapColumnToField(Properties table, String columnName) {
    if (table.containsKey(columnName)) {
      String mapping = table.getProperty(columnName);
      Field field = fieldFactory.valueOfFieldName(mapping);
      if (field != null) {
        LOG.debug("Mapped column '{} to field '{}'.", columnName, mapping);
        return field;
      }
    }
    Field field = fieldFactory.valueOfFieldName(columnName);
    if (field != null) {
      LOG.debug("Column/Field equivalience '{}'.", columnName);
      return field;
    }
    LOG.debug("Failed to map column '{}' to an available field: {}", columnName, fieldFactory.getFieldNames());
    return null;
  }

}
