package fm.last.commons.hive.serde.transform;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class IdentityFieldTransformTest {

  @Test
  public void typical() {
    assertThat((Integer) IdentityFieldTransform.INSTANCE.transform(2, null), is(2));
  }

}
