package jbr.hivecollege.util;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public interface CollegeConst {
  ObjectInspector strOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
  ObjectInspector dateOI = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
  ObjectInspector charOI = PrimitiveObjectInspectorFactory.javaHiveCharObjectInspector;
  ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;

  String[] STU_NAMES = { "id", "firstname", "lastname", "dob", "gender", "phone", "email", "course", "dept" };
  ObjectInspector[] STU_TYPES = { strOI, strOI, strOI, dateOI, charOI, strOI, strOI, strOI, strOI };

  String[] STU_ADDR_NAMES = { "doorno", "street", "city", "state", "country", "pincode" };
  ObjectInspector[] STU_ADDR_TYPES = { intOI, strOI, strOI, strOI, strOI, intOI };

  String[] STU_QUAL_NAMES = { "college", "course", "percentage", "yearofpass" };
  ObjectInspector[] STU_QUAL_TYPES = { strOI, strOI, doubleOI, intOI };

  String[] STAFF_NAMES = { "id", "firstname", "lastname", "dob", "gender", "phone", "email", "dept", "dateofjoin",
      "qualification" };
  ObjectInspector[] STAFF_TYPES = { strOI, strOI, strOI, dateOI, charOI, strOI, strOI, strOI, dateOI, strOI };

}
