package jbr.hivegenericudtf.structOI;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class ReadFileAtInitStructGUDTF extends GenericUDTF {

  private Map<String, String> _data = new LinkedHashMap<String, String>();
  private static Log _log = LogFactory.getLog(ReadFileAtInitStructGUDTF.class.getName());

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    _log.info("at initialize");

    ObjectInspector str = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    List<String> names = new ArrayList<String>();
    List<ObjectInspector> types = new ArrayList<ObjectInspector>();

    try {
      BufferedReader br = new BufferedReader(new FileReader("hdfs-data.txt"));
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

    for (Entry<String, String> e : _data.entrySet()) {
      names.add(e.getKey());
      types.add(str);
    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(names, types);
  }

  @Override
  public void close() throws HiveException {

  }

  public List<Object[]> processData(Object[] args) {
    _log.info("at processData");
    List<Object[]> result = new ArrayList<Object[]>();

    for (Entry<String, String> s : _data.entrySet()) {
      result.add(new Object[] { s.getKey() + " " + s.getValue() });
    }

    return result;
  }

  @Override
  public void process(Object[] arg0) throws HiveException {
    _log.info("at process");
    List<Object[]> result = processData(arg0);
    Iterator<Object[]> resultItr = result.iterator();

    while (resultItr.hasNext()) {
      Object[] obj = resultItr.next();
      forward(obj);
    }
  }

}
