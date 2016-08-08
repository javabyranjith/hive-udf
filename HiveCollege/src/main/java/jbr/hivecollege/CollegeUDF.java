package jbr.hivecollege;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.xpath.XPathExpressionException;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import jbr.hivecollege.struct.Staff;
import jbr.hivecollege.struct.Student;

public class CollegeUDF extends GenericUDTF {
  private PrimitiveObjectInspector xmlData;

  @Override
  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
    xmlData = (StringObjectInspector) argOIs[0];

    List<String> names = new ArrayList<String>();
    List<ObjectInspector> types = new ArrayList<ObjectInspector>();

    Student.initialize(names, types);
    Staff.initialize(names, types);

    return ObjectInspectorFactory.getStandardStructObjectInspector(names, types);
  }

  public List<Object[]> processData(Object[] args) throws XPathExpressionException {
    List<Object[]> results = new ArrayList<Object[]>();
    String xmldata = xmlData.getPrimitiveJavaObject(args[0]).toString();
    List<Object> fields = new ArrayList<Object>();

    fields.add(Student.evaluate(xmldata));
    fields.add(Staff.evaluate(xmldata));

    results.add(fields.toArray(new Object[fields.size()]));
    return results;
  }

  @Override
  public void process(Object[] args) throws HiveException {
    List<Object[]> results = null;

    try {
      results = processData(args);
    } catch (XPathExpressionException e) {
      e.printStackTrace();
    }
    Iterator<Object[]> resultsItr = results.iterator();

    while (resultsItr.hasNext()) {
      Object[] data = resultsItr.next();
      forward(data);
    }
  }

  @Override
  public void close() throws HiveException {

  }

}
