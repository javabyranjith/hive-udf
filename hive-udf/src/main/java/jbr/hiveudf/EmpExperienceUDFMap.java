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
@Description(name = "expmap", value = "_FUNC_(str,str) - find experience", 
extended = "SELECT CxmlToJsonUDF(p.factid, p.authority, p.xmldata, tc.times_cited, \'patentnumber,publicationdate\') FROM patents_20151114 p, times_cited_20151026 tc")
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
