package fm.last.commons.hive.serde;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.ArrayWritable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AbstractReadOnlySerDeTest {

  @Mock
  private FieldFactory mockFieldFactory;
  @Mock
  private Field mockField;
  @Mock
  private ArrayWritable mockRecord;

  private CaptureSerDe serde;
  private Properties hiveTableProperties;

  @Before
  public void setup() throws SerDeException {
    when(mockFieldFactory.valueOfFieldName("one")).thenReturn(mockField);
    hiveTableProperties = new Properties();
    hiveTableProperties.setProperty("columns", "one");
    hiveTableProperties.setProperty("columns.types", "int");

    serde = new CaptureSerDe(mockFieldFactory);
    serde.initialize(null, hiveTableProperties);
  }

  @Test
  public void serializedClass() {
    assertEquals(ArrayWritable.class, serde.getSerializedClass());
  }

  @Test
  public void deserialize() throws SerDeException {
    serde.deserialize(mockRecord);
    assertThat(serde.capturedWritable, is(mockRecord));

    ArrayListRow row = (ArrayListRow) serde.capturedRow;
    assertThat(row.asArrayList().size(), is(1));
    assertThat(row.asArrayList().get(0), is(nullValue()));
  }

  @Test
  public void rowIsReused() throws SerDeException {
    serde.deserialize(mockRecord);
    Row row = serde.capturedRow;

    serde.deserialize(mockRecord);
    assertThat(serde.capturedRow, is(row));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void serialize() throws SerDeException {
    serde.serialize(null, null);
  }

  public class CaptureSerDe extends AbstractReadOnlySerDe<ArrayWritable> {

    private ArrayWritable capturedWritable;
    private Row capturedRow;

    public CaptureSerDe(FieldFactory fieldFactory) {
      super(ArrayWritable.class, fieldFactory);
    }

    @Override
    public void mapRecordIntoRow(ArrayWritable writable, Row row) throws SerDeException {
      capturedWritable = writable;
      capturedRow = row;
    }

  }
}
