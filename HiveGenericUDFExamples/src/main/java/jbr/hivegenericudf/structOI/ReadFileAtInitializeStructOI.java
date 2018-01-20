package jbr.hivegenericudf.structOI;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class ReadFileAtInitializeStructOI extends GenericUDF {
  private Log _log = LogFactory.getLog(ReadFileAtInitializeStructOI.class.getName());
  private PrimitiveObjectInspector _empId, _firstName, _lastName;
  private Map<String, String> _data = new LinkedHashMap<String, String>();

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
    return null;
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    _empId = (StringObjectInspector) args[0];
    _firstName = (StringObjectInspector) args[1];
    _lastName = (StringObjectInspector) args[2];

    try {
      BufferedReader br = new BufferedReader(new FileReader("emp-config.txt"));
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

    List<String> names = new ArrayList<String>();
    List<ObjectInspector> types = new ArrayList<ObjectInspector>();

    for (Entry<String, String> data : _data.entrySet()) {

      if (data.getValue().equalsIgnoreCase("string")) {
        names.add(data.getKey());
        types.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      }

      if (data.getValue().equalsIgnoreCase("int")) {
        names.add(data.getKey());
        types.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
      }

    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(names, types);
  }

}
