package jbr.hiveudf;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TextArrayUDFTest {

  @Test
  public void test() throws Exception {
    TextArrayUDF arrayUDF = new TextArrayUDF();
    Text[] output = arrayUDF.evaluate("ranjith");
    System.out.println(output[0].toString());

  }

}
