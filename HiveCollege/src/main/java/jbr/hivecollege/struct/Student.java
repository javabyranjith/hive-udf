package jbr.hivecollege.struct;

import java.io.StringBufferInputStream;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;

import jbr.hivecollege.util.CollegeConst;

public class Student {
  private static Log _log = LogFactory.getLog("jbr.hivecollege.struct.Student");

  public static void initialize(List<String> names, List<ObjectInspector> types) {
    ObjectInspector students = ObjectInspectorFactory
        .getStandardStructObjectInspector(Arrays.asList(CollegeConst.STU_NAMES), Arrays.asList(CollegeConst.STU_TYPES));

    names.add("students");
    types.add(ObjectInspectorFactory.getStandardListObjectInspector(students));
  }

  public static List<Object> evaluate(String xmldata) {
    List<Object> students = new ArrayList<Object>();

    try {
      Document document = new SAXBuilder().build(new StringBufferInputStream(xmldata));
      String expression = "//college/students/student";
      XPathExpression<Element> xPathExpr = XPathFactory.instance().compile(expression, Filters.element());
      List<Element> studs = xPathExpr.evaluate(document);

      for (Element stud : studs) {
        String id = stud.getAttributeValue("id");
        String fName = stud.getChildText("firstname");
        String lName = stud.getChildText("lastname");
        String dob = stud.getChildText("dob");
        String gender = stud.getChildText("gender");
        String phone = stud.getChildText("phone");
        String email = stud.getChildText("email");
        String course = stud.getChildText("course");
        String dept = stud.getChildText("dept");

        students.add(Arrays.asList(
            new Object[] { id, fName, lName, StringUtils.isNotEmpty(dob) ? new DateWritable(Date.valueOf(dob)) : null,
                StringUtils.isNotEmpty(gender) ? new HiveChar(gender, 1) : null, phone, email, course, dept }));
      }
    } catch (Exception e) {
      _log.info("ERROR at Students UDF: " + e.getMessage());
    }

    return students;
  }

}
