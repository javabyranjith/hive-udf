package jbr.hivegenericudf;

import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class EmpExperienceInfoStructOI extends GenericUDF {
  private Log _log = LogFactory.getLog(EmpExperienceInfoStructOI.class.getName());
  private PrimitiveObjectInspector _empIdOI, _dojOI;

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    _log.info("inside init()");

    // Check the no. of arguments
    if (args.length != 2) {
      throw new UDFArgumentLengthException("This UDF will take max of 2 parameters!!");
    }

    // Inputs
    _empIdOI = (StringObjectInspector) args[0];
    _dojOI = (StringObjectInspector) args[1];

    // Outputs
    List<String> fieldNames = new ArrayList<String>();
    List<ObjectInspector> fieldTypes = new ArrayList<ObjectInspector>();

    fieldNames.add("EMP_ID");
    fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    fieldNames.add("EXPERIENCE");
    fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldTypes);
  }

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    String empId = _empIdOI.getPrimitiveJavaObject(args[0].get()).toString();
    String doj = _dojOI.getPrimitiveJavaObject(args[1].get()).toString();

    SimpleDateFormat format = new SimpleDateFormat("DD-MM-YYYY");
    Date joindate = null;

    try {
      joindate = format.parse(doj);
    } catch (ParseException e) {
      e.printStackTrace();
    }

    long daysTotal = (System.currentTimeMillis() - joindate.getTime()) / (24 * 60 * 60 * 1000);
    BigInteger days = new BigInteger(String.valueOf(daysTotal));
    BigInteger bi[] = days.divideAndRemainder(new BigInteger("365"));

    List<String> exp = new ArrayList<String>();
    exp.add(empId);
    exp.add(bi[0] + " Years, " + bi[1] + " Days");

    return exp;
  }

  @Override
  public String getDisplayString(String[] children) {
    return null;
  }

}
