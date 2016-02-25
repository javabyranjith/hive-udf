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

public class EmpExperienceInfoStructOITest {
  private EmpExperienceInfoStructOI _empStructOI;
  private ObjectInspector _empId, _doj;
  private StructObjectInspector _result;

  @Before
  public void setUp() throws Exception {
    _empStructOI = new EmpExperienceInfoStructOI();
    _empId = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    _doj = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

    _result = (StructObjectInspector) _empStructOI.initialize(new ObjectInspector[] { _empId, _doj });
  }

  @Test
  public void test() throws Exception {
    Object result = _empStructOI
        .evaluate(new DeferredObject[] { new DeferredJavaObject("1000"), new DeferredJavaObject("18-12-2000") });

    List<Object> output = _result.getStructFieldsDataAsList(result);

    Assert.assertEquals("1000", output.get(0));
    Assert.assertEquals("16 Years, 65 Days", output.get(1));
  }

}
