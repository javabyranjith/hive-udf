package jbr.hivegenericudf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class StringStructObjectInspectorGUDF extends GenericUDF {
  PrimitiveObjectInspector _name, _value;

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    _name = (StringObjectInspector) args[0];
    _value = (StringObjectInspector) args[1];

    // Output data
    List<String> fieldNames = new ArrayList<String>();
    List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
    ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

    fieldNames.add("name");
    fieldNames.add("value");

    fieldOIs.add(stringOI);
    fieldOIs.add(stringOI);

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    String name = _name.getPrimitiveJavaObject(args[0].get()).toString();
    String value = _value.getPrimitiveJavaObject(args[1].get()).toString();
    List<String> results = new ArrayList<String>();
    results.add(name);
    results.add(value);

    return results;
  }

  @Override
  public String getDisplayString(String[] children) {
    return null;
  }

}
