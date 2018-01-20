package jbr.hivegenericudf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class TestGUDF extends GenericUDF {

  private PrimitiveObjectInspector _firstName, _lastName, _address;

  @Override
  public void configure(MapredContext context) {
    System.out.println("inside configure()");
    super.configure(context);
  }

  @Override
  public Object evaluate(DeferredObject[] arg0) throws HiveException {
    System.out.println("inside eval()");
    
    return null;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return null;
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    System.out.println("inside init()");
    _firstName = (StringObjectInspector) args[0];
    _lastName = (StringObjectInspector) args[1];
    _address = (StringObjectInspector) args[2];

    List<String> structFieldNames = new ArrayList<String>();
    List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();

    structFieldNames.add("FirstName");
    structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    structFieldNames.add("LastName");
    structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    structFieldNames.add("Address");
    structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
  }

}
