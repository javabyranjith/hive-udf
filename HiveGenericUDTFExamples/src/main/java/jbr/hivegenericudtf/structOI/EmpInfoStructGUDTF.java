package jbr.hivegenericudtf.structOI;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class EmpInfoStructGUDTF extends GenericUDTF {
  private static Log _log = LogFactory.getLog(EmpInfoStructGUDTF.class.getName());

  private PrimitiveObjectInspector _empid, _firstname, _lastname, _dob, _designation;

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    _empid = (StringObjectInspector) args[0];
    _firstname = (StringObjectInspector) args[1];
    _lastname = (StringObjectInspector) args[2];
    _dob = (StringObjectInspector) args[3];
    _designation = (StringObjectInspector) args[4];

    List<String> names = new ArrayList<String>();
    List<ObjectInspector> types = new ArrayList<ObjectInspector>();

    names.add("EMP_ID");
    //types.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    types.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    names.add("FIRST_NAME");
    types.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    names.add("LAST_NAME");
    types.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    names.add("DATE_OF_BIRTH");
    //types.add(PrimitiveObjectInspectorFactory.javaDateObjectInspector);
    types.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    names.add("POSITION");
    types.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    return ObjectInspectorFactory.getStandardStructObjectInspector(names, types);
  }

  @Override
  public void process(Object[] args) throws HiveException {
    List<Object[]> result = new ArrayList<Object[]>();

    //int empid = Integer.valueOf(_empid.getPrimitiveJavaObject(args[0]).toString());
    String empid = _empid.getPrimitiveJavaObject(args[0]).toString();
    String firstname = _firstname.getPrimitiveJavaObject(args[1]).toString();
    String lastname = _lastname.getPrimitiveJavaObject(args[2]).toString();
    String dob = _dob.getPrimitiveJavaObject(args[3]).toString();

    /*SimpleDateFormat format = new SimpleDateFormat("DD-MM-YYYY");
    Date dob = null;
    try {
      dob = format.parse(_dob.getPrimitiveJavaObject(args[3]).toString());
    } catch (ParseException e) {
      e.printStackTrace();
    }
*/
    String desig = _designation.getPrimitiveJavaObject(args[4]).toString();

    result.add(new Object[] { empid + "1", firstname + "2", lastname + "3", dob + "4", desig + "5" });

    Iterator<Object[]> itr = result.iterator();
    while (itr.hasNext()) {
      Object[] objArr = itr.next();
      forward(objArr);
    }
  }

  @Override
  public void close() throws HiveException {

  }

}
