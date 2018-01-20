package jbr.hiveudf;

import java.util.List;

import org.junit.Test;

public class EmpExperienceUDFListTest {

  @Test
  public void test() throws Exception {
    EmpExperienceUDFList list = new EmpExperienceUDFList();
    List<String> output = list.evaluate("Ranjith");
    System.out.println(output);
  }

}
