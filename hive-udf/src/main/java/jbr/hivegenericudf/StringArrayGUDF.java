package jbr.hivegenericudf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class StringArrayGUDF extends GenericUDF {

  StringObjectInspector _input;

  @Override
  public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    _input = (StringObjectInspector) args[0];
    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    String input = _input.getPrimitiveJavaObject(args[0].get()).toString();
    char[] chars = input.toCharArray();
    String[] output = new String[chars.length];

    for (int i = 0; i < chars.length; i++) {
      output[i] = String.valueOf(chars[i]).toString();
    }
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return null;
  }

}
