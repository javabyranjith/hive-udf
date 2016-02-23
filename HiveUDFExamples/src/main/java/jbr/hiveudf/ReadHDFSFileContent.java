package jbr.hiveudf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;

public class ReadHDFSFileContent extends UDF {
  private FileStatus _fileStatus[];
  private FileSystem _fileSystem;

  public String evaluate(String hdfsPath) {
    System.out.println("enter evaluate()");
    String result = "";

    try {
      _fileSystem = FileSystem.get(new Configuration());
      if (_fileSystem.isDirectory(new Path(hdfsPath))) {
        result = evaluateDirectory(hdfsPath);
      } else if (_fileSystem.isFile(new Path(hdfsPath))) {
        result = evaluateFile(new Path(hdfsPath));
      }
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println("exit evaluate()");
    return result;
  }

  /**
   * Read single file content
   * 
   * @param hdfsFileName
   * @return
   */
  public String evaluateFile(Path hdfsFileName) {
    StringBuffer result = new StringBuffer();

    try {
      FSDataInputStream dataInputStream = _fileSystem.open(hdfsFileName);
      BufferedReader br = new BufferedReader(new InputStreamReader(dataInputStream));
      String line;

      while ((line = br.readLine()) != null) {
        result.append(line).append(" ");
      }

    } catch (Exception e) {
      System.out.println(e.getMessage());
    }

    return result.toString();
  }

  /**
   * Read all files from a directory.
   * 
   * @param hdfsDirectory
   * @return
   */
  public String evaluateDirectory(String hdfsDirectory) {
    StringBuffer result = new StringBuffer();

    try {
      _fileStatus = _fileSystem.listStatus(new Path(hdfsDirectory.toString()));

      for (FileStatus fileStat : _fileStatus) {
        result.append(evaluateFile(fileStat.getPath())).append("\n");
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }

    return result.toString();
  }

}
