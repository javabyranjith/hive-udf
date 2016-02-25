package jbr.hivegenericudf;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 * An Example UDF for StructObjectInspector with GenericUDF.
 */
public class StructObjectInspectorGenericUDF extends GenericUDF {
  private Log _log = LogFactory.getLog(StructObjectInspectorGenericUDF.class.getName());
  private PrimitiveObjectInspector _empId, _firstName, _lastName;

  @Override
  public void configure(MapredContext context) {
    _log.info("inside configure()");
    super.configure(context);
  }

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    _log.info("inside eval()");

    String empid = _empId.getPrimitiveJavaObject(args[0].get()).toString();
    String firstName = _firstName.getPrimitiveJavaObject(args[1].get()).toString();
    String lastName = _lastName.getPrimitiveJavaObject(args[2].get()).toString();

    List<String> result = new ArrayList<String>();
    result.add(empid);
    result.add(firstName);
    result.add(lastName);

    return result;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "StructObjectInspectorGenericUDF";
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    _log.info("inside init()");

    // Check the no. of arguments
    if (args.length != 3) {
      throw new UDFArgumentLengthException("This UDF will take max of 3 parameters!!");
    }

    // Input Types
    _empId = (StringObjectInspector) args[0];
    _firstName = (StringObjectInspector) args[1];
    _lastName = (StringObjectInspector) args[2];

    // Output Types
    List<String> fieldNames = new ArrayList<String>();
    List<ObjectInspector> fieldTypes = new ArrayList<ObjectInspector>();

    fieldNames.add("EmpId");
    fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    fieldNames.add("FirstName");
    fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    fieldNames.add("LastName");
    fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldTypes);
  }

}
