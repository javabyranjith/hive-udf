package jbr.hiveudf;

import java.text.ParseException;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * UDF which returns the Map value for input.
 */
public class TextArrayUDF extends UDF {

  public Text[] evaluate(String firstname) throws ParseException {
    char[] chars = firstname.toCharArray();
    Text[] output = new Text[chars.length];

    for (int i = 0; i < chars.length; i++) {
      output[i] = new Text(String.valueOf(chars[i]));
    }

    return output;
  }
}
