package jbr.hivegenericudf;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import jbr.hivegenericudf.StringArrayGUDF;

public class StringArrayGUDFTest {

  @Test
  public void test() throws Exception {

    StringArrayGUDF stringArrayGenericUDF = new StringArrayGUDF();
    ObjectInspector input = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

    stringArrayGenericUDF.initialize(new ObjectInspector[] { input });

    String[] output = (String[]) stringArrayGenericUDF
        .evaluate(new DeferredObject[] { new DeferredJavaObject("ranjith") });
    System.out.println(output);
    stringArrayGenericUDF.close();
  }

}
