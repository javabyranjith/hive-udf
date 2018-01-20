package jbr.hivegenericudtf.structOI;

import java.io.IOException;
import java.io.StringBufferInputStream;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class LibraryStruct extends GenericUDTF {
  private static Log _log = LogFactory.getLog(LibraryStruct.class.getName());

  private StringObjectInspector _data;

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    _data = (StringObjectInspector) args[0];

    List<String> names = new ArrayList<String>();
    List<ObjectInspector> types = new ArrayList<ObjectInspector>();

    ObjectInspector strOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    ObjectInspector dateOI = PrimitiveObjectInspectorFactory.writableDateObjectInspector;

    names.add("id");
    types.add(strOI);
    names.add("category");
    types.add(strOI);
    names.add("department");
    types.add(strOI);
    names.add("title");
    types.add(strOI);
    names.add("isbn");
    types.add(intOI);
    names.add("price");
    types.add(strOI);
    names.add("publisher");
    types.add(strOI);
    names.add("publishdate");
    types.add(dateOI);

    // author
    List<String> auNames = new ArrayList<String>();
    List<ObjectInspector> auTypes = new ArrayList<ObjectInspector>();
    auNames.add("firstname");
    auTypes.add(strOI);
    auNames.add("lastname");
    auTypes.add(strOI);

    List<String> auAddrNames = new ArrayList<String>();
    List<ObjectInspector> auAddrTypes = new ArrayList<ObjectInspector>();
    auAddrNames.add("doorno");
    auAddrTypes.add(intOI);
    auAddrNames.add("city");
    auAddrTypes.add(strOI);
    auAddrNames.add("state");
    auAddrTypes.add(strOI);
    auAddrNames.add("country");
    auAddrTypes.add(strOI);

    ObjectInspector address = ObjectInspectorFactory.getStandardStructObjectInspector(auAddrNames, auAddrTypes);
    auNames.add("address");
    auTypes.add(address);

    ObjectInspector author = ObjectInspectorFactory.getStandardStructObjectInspector(auNames, auTypes);
    names.add("author");
    types.add(author);

    return ObjectInspectorFactory.getStandardStructObjectInspector(names, types);
  }

  public List<Object[]> processData(Object[] args) throws HiveException {
    List<Object[]> result = new ArrayList<Object[]>();
    List<Object> fields = new ArrayList<Object>();
    String xml = _data.getPrimitiveJavaObject(args[0]).toString();

    fields.add(getData(xml));

    result.add(fields.toArray(new Object[fields.size()]));

    return result;
  }

  @Override
  public void process(Object[] args) throws HiveException {
    List<Object[]> result = processData(args);

    Iterator<Object[]> itr = result.iterator();
    while (itr.hasNext()) {
      Object[] objArr = itr.next();
      forward(objArr);
    }
  }

  private List<Object> getData(String xml) {
    String xpath = "book";
    List<Object> values = new ArrayList<Object>();

    try {
      DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = builderFactory.newDocumentBuilder();
      Document document = builder.parse(new StringBufferInputStream(xml));
      XPath xPath = XPathFactory.newInstance().newXPath();

      NodeList book = (NodeList) xPath.compile("book").evaluate(document, XPathConstants.NODESET);
      for (int i = 0; null != book && book.getLength() > 0 && i < book.getLength(); i++) {
        Node node = book.item(i);
        NodeList bookChildren = node.getChildNodes();

        String id = "", category = "", department = "", title = "", firstname = "", lastname = "", city = "",
            state = "", country = "", publisher = "", price = "", isbn = "";
        Integer doorno = null, phone = null;
        DateWritable publishdate = null;

        // Attributes
        NamedNodeMap attr = node.getAttributes();
        for (int a = 0; null != attr && a < attr.getLength(); a++) {
          Node n = (Attr) attr.item(a);
          if (n.getNodeName().equals("id")) id = n.getTextContent();
          if (n.getNodeName().equals("category")) category = n.getTextContent();
          if (n.getNodeName().equals("dept")) department = n.getTextContent();
        }

        for (int j = 0; null != bookChildren && bookChildren.getLength() > 0 && j < bookChildren.getLength(); j++) {
          Node bookNode = bookChildren.item(j);
          String nodeName = bookNode.getNodeName();
          String content = bookNode.getTextContent();

          // Title
          if (nodeName.equals("title")) {
            title = content;
            continue;
          }

          // Author
          if (nodeName.equals("author")) {
            NodeList author = bookNode.getChildNodes();

            for (int k = 0; null != author && author.getLength() > 0 && k < author.getLength(); k++) {
              Node auNode = author.item(k);
              String auNodeName = auNode.getNodeName();
              String auNodeVal = auNode.getTextContent();
              if (auNodeName.equals("firstname")) firstname = auNodeVal;
              if (auNodeName.equals("lastname")) lastname = auNodeVal;

              // Author Address
              if (auNodeName.equals("address")) {
                NodeList authorChidren = author.item(k).getChildNodes();
                for (int l = 0; null != authorChidren && authorChidren.getLength() > 0
                    && l < authorChidren.getLength(); l++) {
                  Node auAddrChild = authorChidren.item(l);
                  String auAddrContent = auAddrChild.getTextContent();
                  String auAddrNodeName = auAddrChild.getNodeName();

                  if ("doorno".equals(auAddrNodeName)) doorno = Integer.valueOf(auAddrContent);
                  if ("city".equals(auAddrNodeName)) city = auAddrContent;
                  if ("state".equals(auAddrNodeName)) state = auAddrContent;
                  if ("country".equals(auAddrNodeName)) country = auAddrContent;
                } // author address for
                if (auNodeName.equals("phone")) phone = Integer.valueOf(auNodeVal);
              } // author address if
            } // author for
          } // author if
          if (nodeName.equals("publisher")) publisher = content;
          if (nodeName.equals("publishdate")) publishdate = new DateWritable(Date.valueOf(content));
          if (nodeName.equals("price")) price = content;
          if (nodeName.equals("isbn")) isbn = content;
        }

        values.addAll(Arrays.asList(id, category, department, title, firstname, lastname, doorno, city, state, country,
            phone, publisher, publishdate, price, isbn));
      }
    } catch (ParserConfigurationException | SAXException | IOException | XPathExpressionException e) {
      _log.error("ERROR - " + e.getMessage());
    }

    return new ArrayList<Object>(values);
  }

  @Override
  public void close() throws HiveException {

  }

}
