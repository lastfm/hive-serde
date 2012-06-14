package fm.last.commons.hive.serde;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@RunWith(MockitoJUnitRunner.class)
public class ArrayListRowTest {

  @Mock
  private Field mockField;
  @Mock
  private FieldTransform mockFieldTransform;
  @Mock
  private TableDefinition mockTableDefinition;

  private List<String> columnNames;
  private List<TypeInfo> columnTypes;
  private ArrayListRow row;

  @Before
  public void setup() {
    columnNames = Lists.newArrayList("one", "two", "three");
    columnTypes = Lists.newArrayList(TypeInfoFactory.intTypeInfo, TypeInfoFactory.intTypeInfo,
        TypeInfoFactory.intTypeInfo);
    when(mockTableDefinition.getColumnIdsForField(mockField)).thenReturn(Sets.newHashSet(1, 3));
    when(mockTableDefinition.getColumnNames()).thenReturn(columnNames);
    when(mockTableDefinition.getColumnTypes()).thenReturn(columnTypes);
    when(mockField.getTransform()).thenReturn(mockFieldTransform);
    when(mockFieldTransform.transform(any(), any(TypeInfo.class))).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return invocation.getArguments()[0];
      }
    });
    row = new ArrayListRow(mockTableDefinition);
  }

  @Test
  public void typicalSetField() {
    when(mockTableDefinition.getColumnIdsForField(mockField)).thenReturn(Sets.newHashSet(0, 2));
    row.setField(mockField, 10);
    List<Object> cells = row.asArrayList();
    assertThat((Integer) cells.get(0), is(10));
    assertThat(cells.get(1), is(nullValue()));
    assertThat((Integer) cells.get(2), is(10));
  }

  @Test
  public void asArrayList() {
    List<Object> cells = row.asArrayList();
    assertThat(cells.size(), is(3));
    assertThat(cells.get(0), is(nullValue()));
    assertThat(cells.get(1), is(nullValue()));
    assertThat(cells.get(2), is(nullValue()));
  }

}
