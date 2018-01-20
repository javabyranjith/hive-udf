package jbr.hiveudf;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class EmpBonusUDFList extends UDF {
  FileStatus _fileStatus[];
  FileSystem _fileSystem;

  public List<String> evaluate(String empid) throws HiveException {
    List<String> list = new ArrayList<String>();
    String hdfsFileName = "/user/BoldGuest/ranjith/hive/data/emp/empbonus.txt";

    try {
      FSDataInputStream dataInputStream = _fileSystem.open(new Path(hdfsFileName));
      BufferedReader br = new BufferedReader(new InputStreamReader(dataInputStream));
      String line;

      while ((line = br.readLine()) != null) {
        String[] value = line.split(",");
        String empidInFile = value[0];
        String bonus = value[1];

        if (StringUtils.isNotEmpty(empidInFile) && empid.equalsIgnoreCase(empidInFile)) {
          list.add(empidInFile);
          list.add(bonus);
          break;
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    return list;
  }
}