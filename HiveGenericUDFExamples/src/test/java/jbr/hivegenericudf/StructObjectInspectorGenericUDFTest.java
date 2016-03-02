package jbr.hivegenericudf;

import java.util.List;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import jbr.hivegenericudf.structOI.EmpInfoStructOI;

public class StructObjectInspectorGenericUDFTest {
  private EmpInfoStructOI _structOIGUDF;
  private ObjectInspector _empId, _firstName, _lastName;
  private StructObjectInspector _result;

  @Before
  public void setUp() throws Exception {
    _structOIGUDF = new EmpInfoStructOI();
    _empId = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    _firstName = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    _lastName = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

    _result = (StructObjectInspector) _structOIGUDF.initialize(new ObjectInspector[] { _empId, _firstName, _lastName });
  }

  @Test
  public void test() throws Exception {

    Object result = _structOIGUDF.evaluate(new DeferredObject[] { new DeferredJavaObject("1000"),
        new DeferredJavaObject("Ranjith"), new DeferredJavaObject("Sekar") });

    List<Object> output = _result.getStructFieldsDataAsList(result);

    Assert.assertEquals("1000", output.get(0));
    Assert.assertEquals("Ranjith", output.get(1));
    Assert.assertEquals("Sekar", output.get(2));
  }

}
