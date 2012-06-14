package fm.last.commons.hive.serde.transform;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Before;
import org.junit.Test;

import fm.last.commons.hive.serde.FieldTransform;

public class UnixTimeFieldTransformTest {

  private FieldTransform transform;

  @Before
  public void setup() {
    transform = new UnixTimeFieldTransform();
  }

  @Test
  public void nonStringColumn() {
    assertThat((Integer) transform.transform(948848, TypeInfoFactory.intTypeInfo), is(948848));
  }

  @Test
  public void stringColumn() {
    assertThat((String) transform.transform(2, TypeInfoFactory.stringTypeInfo), is("1970-01-01 00:00:02"));
  }

}
