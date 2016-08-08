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
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;

import jbr.hivecollege.util.CollegeConst;

public class Staff {
  private static Log _log = LogFactory.getLog("jbr.hivecollege.struct.Staff");

  public static void initialize(List<String> names, List<ObjectInspector> types) {
    ObjectInspector staffs = ObjectInspectorFactory.getStandardStructObjectInspector(
        Arrays.asList(CollegeConst.STAFF_NAMES), Arrays.asList(CollegeConst.STAFF_TYPES));

    names.add("staffs");
    types.add(ObjectInspectorFactory.getStandardListObjectInspector(staffs));
  }

  public static List<Object> evaluate(String xmldata) {
    List<Object> staffs = new ArrayList<Object>();

    try {
      Document document = new SAXBuilder().build(new StringBufferInputStream(xmldata));
      String expression = "//college/staffs/staff";
      XPathExpression<Element> xPathExpr = XPathFactory.instance().compile(expression, Filters.element());
      List<Element> stafs = xPathExpr.evaluate(document);

      for (Element staf : stafs) {
        String id = staf.getAttributeValue("id");
        String fName = staf.getChildText("firstname");
        String lName = staf.getChildText("lastname");
        String dob = staf.getChildText("dob");
        String gender = staf.getChildText("gender");
        String phone = staf.getChildText("phone");
        String email = staf.getChildText("email");
        String dept = staf.getChildText("dept");
        String dateofjoin = staf.getChildText("dateofjoin");
        String qualification = staf.getChildText("qualification");

        staffs.add(Arrays.asList(new Object[] { id, fName, lName,
            StringUtils.isNotEmpty(dob) ? new DateWritable(Date.valueOf(dob)) : null,
            StringUtils.isNotEmpty(gender) ? new HiveChar(gender, 1) : null, phone, email, dept,
            StringUtils.isNotEmpty(dateofjoin) ? new DateWritable(Date.valueOf(dateofjoin)) : null, qualification }));
      }
    } catch (Exception e) {
      _log.info("ERROR at Staff UDF: " + e.getMessage());
    }

    return staffs;
  }
}
