package jbr.hivegenericudf.mapOI;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class ReadFileAtConfigureMethodGUDF extends GenericUDF {

  private Map<String, String> _data = new LinkedHashMap<String, String>();

  @Override
  public void configure(MapredContext context) {
    try {
      BufferedReader br = new BufferedReader(new FileReader("sample-data.txt"));
      String line;

      while ((line = br.readLine()) != null) {
        String[] config = StringUtils.split(line, "|");
        _data.put(config[0], config[1]);
      }
      br.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    super.configure(context);
  }

  @Override
  public Object evaluate(DeferredObject[] arg0) throws HiveException {
    return _data;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return null;
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {

    ObjectInspector str = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    return ObjectInspectorFactory.getStandardMapObjectInspector(str, str);
  }

}