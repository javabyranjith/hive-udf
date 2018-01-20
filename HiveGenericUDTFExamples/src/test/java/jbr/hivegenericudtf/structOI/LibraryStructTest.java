package jbr.hivegenericudtf.structOI;

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

public class LibraryStructTest {
  private ObjectInspector _dataOI;
  private LibraryStruct _libraryStruct;

  @Before
  public void setUp() throws Exception {
    _libraryStruct = new LibraryStruct();
    _dataOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    BufferedReader reader = new BufferedReader(new FileReader(new File("testdata/library/library1.txt")));
    String xml = reader.readLine().split("\t")[2];

    StructObjectInspector init = _libraryStruct.initialize(new ObjectInspector[] { _dataOI });
    List<? extends StructField> fields = init.getAllStructFieldRefs();

    List<Object[]> result = _libraryStruct.processData(new Object[] { xml });
    System.out.println(result.get(0).length);
    for (int i = 0; i < result.get(0).length; i++) {
      System.out.println(fields.get(i).getFieldName() + " >> " + result.get(0)[i]);
    }

    reader.close();

  }

  @Test
  public void test() {

  }

}
