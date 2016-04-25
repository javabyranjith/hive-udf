package jbr.hivegenericudtf.structOI;

import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EmpInfoStructGUDTFTest {
  private EmpInfoStructGUDTF _empInfoStructGUDTF;
  private ObjectInspector _empid, _firstname, _lastname, _dob, _designation;

  @Before
  public void setUp() throws Exception {
    _empInfoStructGUDTF = new EmpInfoStructGUDTF();
    _empid = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    _firstname = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    _lastname = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    _dob = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    _designation = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

    _empInfoStructGUDTF.initialize(new ObjectInspector[] { _empid, _firstname, _lastname, _dob, _designation });
  }

  @Test
  public void test() throws Exception {
    List<Object[]> result = _empInfoStructGUDTF
        .processData(new Object[] { "100", "Ranjith", "Sekar", "10-12-2000", "Engineer" });

    for (Object obj : result.get(0)) {
      System.out.println(obj.toString());
    }

    Assert.assertEquals("empid: 100", result.get(0)[0]);
    Assert.assertEquals("firstname: Ranjith", result.get(0)[1]);
    Assert.assertEquals("lastname: Sekar", result.get(0)[2]);
    Assert.assertEquals("date of birth: 10-12-2000", result.get(0)[3]);
    Assert.assertEquals("position: Engineer", result.get(0)[4]);

  }

}
