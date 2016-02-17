package jbr.hiveudf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * UDF which returns String value for the Input by reading the files in the HDFS
 * to calculate the Bonus.
 */
@Description(name = "bonus", value = "_FUNC_(str,str) - find bonus", extended = "SELECT bonus(column,column) from empinfo")
public class EmpBonusUDF extends UDF {
  static final Log _log = LogFactory.getLog(EmpBonusUDF.class);
  FileStatus _fileStatus[];
  FileSystem _fileSystem;

  public String evaluate(String empid, String hdfsPath) {
    _log.info("inside evaluate()");
    String result = "";

    try {
      _fileSystem = FileSystem.get(new Configuration());
      if (_fileSystem.isDirectory(new Path(hdfsPath))) {
        result = evaluateDirectory(empid.toString(), hdfsPath);
      } else if (_fileSystem.isFile(new Path(hdfsPath))) {
        result = evaluateFile(empid.toString(), hdfsPath);
      }
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return result;
  }

  public String evaluateFile(String empid, String hdfsFileName) {
    _log.info("inside evaluateFile()");
    String result = "";

    try {
      FSDataInputStream dataInputStream = _fileSystem.open(new Path(hdfsFileName));
      BufferedReader br = new BufferedReader(new InputStreamReader(dataInputStream));
      String line;

      while ((line = br.readLine()) != null) {
        String[] value = line.split(",");
        String empidInFile = value[0];
        String bonus = value[1];

        _log.info("empidInFile: " + empidInFile + " >> " + "bonus: " + bonus);

        if (StringUtils.isNotEmpty(empidInFile) && empid.toString().equalsIgnoreCase(empidInFile)) {
          result = empidInFile + " : " + bonus;
          break;
        }
      }

    } catch (Exception e) {
      _log.error("Error on getCitingPatCount(): " + e.getMessage());
    }

    return StringUtils.isEmpty(result) ? empid.toString() : result;
  }

  public String evaluateDirectory(String empid, String hdfsLoc) {
    _log.info("inside evaluate()");
    String result = "";

    try {
      _fileStatus = _fileSystem.listStatus(new Path(hdfsLoc.toString()));

      for (FileStatus fileStat : _fileStatus) {
        BufferedReader br = new BufferedReader(new InputStreamReader(_fileSystem.open(fileStat.getPath())));
        String line;

        while ((line = br.readLine()) != null) {
          String value[] = line.split(",");
          String empidInFile = value[0];
          String bonus = value[1];

          _log.info("empidInFile: " + empidInFile + " >> " + "bonus: " + bonus);

          if (StringUtils.isNotEmpty(empidInFile) && empid.toString().equalsIgnoreCase(empidInFile)) {
            result = empidInFile + " : " + bonus;
            break;
          }
        }
      }
    } catch (Exception e) {
      _log.error((new StringBuilder("Error on : ")).append(e.getMessage()).toString());
    }

    return StringUtils.isEmpty(result) ? empid.toString() : result;
  }

}
