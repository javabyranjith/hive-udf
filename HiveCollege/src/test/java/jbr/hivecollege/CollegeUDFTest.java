package jbr.hivecollege;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

public class CollegeUDFTest {
  private ObjectInspector data;
  private CollegeUDF collegeUdf;

  @Before
  public void setUp() throws Exception {
    data = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    collegeUdf = new CollegeUDF();
  }

  @Test
  public void testStructFV() throws Exception {
    File dataXml = new File("testdata/input/admission2015");
    BufferedReader reader = new BufferedReader(new FileReader(dataXml));
    String[] inputData = reader.readLine().split("\t");

    StructObjectInspector init = collegeUdf.initialize(new ObjectInspector[] { data });
    List<? extends StructField> fields = init.getAllStructFieldRefs();
    List<Object[]> result = collegeUdf.processData(new Object[] { inputData[4] });

    System.out.println(result.get(0).length);
    for (int i = 0; i < result.get(0).length; i++) {
      System.out.println(fields.get(i).getFieldName() + ":" + fields.get(i).getFieldObjectInspector().getTypeName()
          + "--> " + result.get(0)[i]);
    }

    reader.close();
  }

}
