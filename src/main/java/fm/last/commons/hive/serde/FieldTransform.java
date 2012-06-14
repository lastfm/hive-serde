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

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Transforms a value located at a {@link Field} into the Hive compatible cell type.
 */
public interface FieldTransform {

  /**
   * @param value The value derived from a {@link Field}.
   * @param columnType The column type declared on the Hive table.
   * @return A value compatible with the declared Hive column type.
   */
  Object transform(Object value, TypeInfo columnType);

}
