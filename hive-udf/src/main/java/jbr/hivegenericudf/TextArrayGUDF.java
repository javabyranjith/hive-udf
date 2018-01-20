package jbr.hivegenericudf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

public class TextArrayGUDF extends GenericUDF {

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
    Text[] output = new Text[chars.length];

    for (int i = 0; i < chars.length; i++) {
      output[i] = new Text(String.valueOf(chars[i]));
    }
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return null;
  }

}
