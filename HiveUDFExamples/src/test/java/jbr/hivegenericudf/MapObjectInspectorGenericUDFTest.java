package jbr.hivegenericudf;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MapObjectInspectorGenericUDFTest {

  private MapObjectInspectorGenericUDF _mapOIGUDF;
  private ObjectInspector _empId, _firstName, _lastName;
  private MapObjectInspector _result;

  @Before
  public void setUp() throws Exception {
    _mapOIGUDF = new MapObjectInspectorGenericUDF();
    _empId = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    _firstName = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    _lastName = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

    _result = (MapObjectInspector) _mapOIGUDF.initialize(new ObjectInspector[] { _empId, _firstName, _lastName });
  }

  @Test
  public void test() throws Exception {
    Object result = _mapOIGUDF.evaluate(new DeferredObject[] { new DeferredJavaObject("1000"),
        new DeferredJavaObject("Ranjith"), new DeferredJavaObject("Sekar") });

    Assert.assertEquals("1000", _result.getMap(result).get("EMP_ID"));
    Assert.assertEquals("Ranjith", _result.getMap(result).get("FIRST_NAME"));
    Assert.assertEquals("Sekar", _result.getMap(result).get("LAST_NAME"));
  }

}
