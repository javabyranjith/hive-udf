package jbr.hiveudf;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class ReadAwsS3FileContent extends UDF {

  /**
   * 
   * @param accessKey - access key of AWS.
   * @param secretKey - secret key of AWS.
   * @param bucketName - bucket name, i.e name of the bucket (e.g: mybucket)
   * @param fileKey - file path, i.e under bucket
   *          (myfolder1/myfolder2/myfile1.txt)
   * @return
   */
  public String evaluate(String accessKey, String secretKey, String bucketName, String fileKey) throws Exception {
    AmazonS3 amazonS3 = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
    S3Object s3Object = amazonS3.getObject(new GetObjectRequest(bucketName, fileKey));
    BufferedReader br = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));

    String line;
    while ((line = br.readLine()) != null) {
      System.out.println(line);
    }

    return "";
  }
}
