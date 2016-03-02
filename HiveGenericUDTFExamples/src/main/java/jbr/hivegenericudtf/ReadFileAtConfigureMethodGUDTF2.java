package jbr.hivegenericudtf;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class ReadFileAtConfigureMethodGUDTF2 extends GenericUDTF {
  List<String> _fnameList = new ArrayList<String>();
  List<String> _lnameList = new ArrayList<String>();
  private PrimitiveObjectInspector _empid, _firstname, _lastname, _dob, _designation;

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    _empid = (StringObjectInspector) args[0];
    _firstname = (StringObjectInspector) args[1];
    _lastname = (StringObjectInspector) args[2];
    _dob = (StringObjectInspector) args[3];
    _designation = (StringObjectInspector) args[4];

    ObjectInspector str = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    List<String> names = new ArrayList<String>();
    List<ObjectInspector> types = new ArrayList<ObjectInspector>();

    try {
      BufferedReader br = new BufferedReader(new FileReader("hdfs-data.txt"));
      String line;

      while ((line = br.readLine()) != null) {
        String[] config = StringUtils.split(line, "|");
        _fnameList.add(config[0]);
        _lnameList.add(config[1]);
      }
      br.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    for (String st : _fnameList) {
      names.add(st);
      types.add(str);
    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(names, types);
  }

  @Override
  public void close() throws HiveException {

  }

  public List<Object[]> processData(Object[] args) {
    List<Object[]> result = new ArrayList<Object[]>();

    String empid = _empid.getPrimitiveJavaObject(args[0]).toString();
    String firstname = _firstname.getPrimitiveJavaObject(args[1]).toString();
    String lastname = _lastname.getPrimitiveJavaObject(args[2]).toString();
    String dob = _dob.getPrimitiveJavaObject(args[3]).toString();
    String desig = _designation.getPrimitiveJavaObject(args[4]).toString();

    result.add(new Object[] { empid + "1", firstname + "2", lastname + "3", dob + "4", desig + "5" });

    return result;
  }

  @Override
  public void process(Object[] arg0) throws HiveException {
    List<Object[]> result = processData(arg0);
    Iterator<Object[]> resultItr = result.iterator();

    while (resultItr.hasNext()) {
      Object[] obj = resultItr.next();
      forward(obj);
    }
  }

}
