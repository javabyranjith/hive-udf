package jbr.hiveudf;

import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * UDF which returns the Map value for input.
 */
@Description(name = "experience_map", value = "_FUNC_(str,str) - find employee experience", extended = "SELECT experience_map(column,column) from empinfo")
public class EmpExperienceUDFMap extends UDF {

  public Map<String, String> evaluate(String empid, String doj) throws ParseException {
    SimpleDateFormat format = new SimpleDateFormat("DD-MM-YYYY");
    Date joindate = format.parse(doj.toString());
    long daysTotal = (System.currentTimeMillis() - joindate.getTime()) / (24 * 60 * 60 * 1000);
    BigInteger days = new BigInteger(String.valueOf(daysTotal));
    BigInteger bi[] = days.divideAndRemainder(new BigInteger("365"));

    Map<String, String> map = new HashMap<String, String>();
    map.put(empid.toString(), bi[0] + " Years, " + bi[1] + " Days");

    return map;
  }
}
