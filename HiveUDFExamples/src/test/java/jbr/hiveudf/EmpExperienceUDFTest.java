package jbr.hiveudf;

import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EmpExperienceUDFTest {

  private EmpExperienceUDFString udfString;

  @Before
  public void setUp() throws Exception {
    udfString = new EmpExperienceUDFString();
  }

  @Test
  public void test() throws Exception {
    String[] output = udfString.evaluate(new Text("100"), new Text("10-12-2013")).split("\t");

    Assert.assertEquals("100", output[0]);
    Assert.assertEquals("3 Years, 57 Days", output[1]);
  }

  @After
  public void tearDown() throws Exception {
  }

}
