package jbr.hivegenericudf;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import jbr.hivegenericudf.TextArrayGUDF;

public class TextArrayGUDFTest {

  @Test
  public void test() throws Exception {
    TextArrayGUDF arrayGUDF = new TextArrayGUDF();
    ObjectInspector input = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

    arrayGUDF.initialize(new ObjectInspector[] { input });

    Text[] output = (Text[]) arrayGUDF.evaluate(new DeferredObject[] { new DeferredJavaObject("ra") });
    System.out.println(output[0].toString());
    arrayGUDF.close();
  }

}
