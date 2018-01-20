package jbr.hiveudf.so;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ParseUrlUDF extends UDF {

  public String evaluate(Text url1, Text url2) {
    String result = "";

    return url1 + " > " + url2;
  }
}