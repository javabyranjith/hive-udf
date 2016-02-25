package jbr.hiveudf;

import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EmpExperienceUDFTest {

  EmpExperienceUDFString udfString;

  @Before
  public void setUp() throws Exception {
    udfString = new EmpExperienceUDFString();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void test() throws Exception {
    System.out.println(udfString.evaluate(new Text("100"), new Text("10-12-2013")));
  }

}
