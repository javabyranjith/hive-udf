package jbr.hiveudf;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * UDF which returns the List of values for input.
 */
@Description(name = "emp_exp_list", value = "_FUNC_(str) - find experience", extended = "SELECT emp_exp_list(column) from empinfo")
public class EmpExperienceUDFList extends UDF {

  public List<String> evaluate(String firstname) throws ParseException {
    char[] chars = firstname.toCharArray();
    List<String> list = new ArrayList<String>();
    for (char s : chars) {
      list.add(String.valueOf(s));
    }

    return list;
  }
}
