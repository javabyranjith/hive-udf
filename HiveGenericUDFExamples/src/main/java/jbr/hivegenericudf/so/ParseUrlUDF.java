package jbr.hivegenericudf.so;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class ParseUrlUDF extends GenericUDF {

  private Log _log = LogFactory.getLog(ParseUrlUDF.class.getName());
  private PrimitiveObjectInspector _url1, _url2;

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    _log.info("inside eval()");

    String url1 = _url1.getPrimitiveJavaObject(args[0].get()).toString();
    String url2 = _url2.getPrimitiveJavaObject(args[1].get()).toString();

    // return url1 + " > " + url2;
    return "test";
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "";
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    _log.info("inside init()");

    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

}
