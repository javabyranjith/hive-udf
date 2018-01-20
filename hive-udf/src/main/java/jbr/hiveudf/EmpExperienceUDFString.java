package jbr.hiveudf;

import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * UDF which returns the String value for input.
 * Refer EmpExperienceUDFString.howtorun file to run this program.
 */
public class EmpExperienceUDFString extends UDF {

  public String evaluate(Text empid, Text doj) throws ParseException {
    SimpleDateFormat format = new SimpleDateFormat("DD-MM-YYYY");
    
    Date joindate = format.parse(doj.toString());
    long daysTotal = (System.currentTimeMillis() - joindate.getTime()) / (24 * 60 * 60 * 1000);
    BigInteger days = new BigInteger(String.valueOf(daysTotal));
    BigInteger bi[] = days.divideAndRemainder(new BigInteger("365"));

    return empid.toString() + "\t" + bi[0] + " Years, " + bi[1] + " Days";
  }
}
