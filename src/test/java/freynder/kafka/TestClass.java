package freynder.kafka;

public class TestClass {

  private String testString;
  private int testInt;
  private long testLong;
  private OtherTestClass otherTestClass;
  public TestClass(String testString, int testInt, long testLong, OtherTestClass otherTestClass) {
    super();
    this.testString = testString;
    this.testInt = testInt;
    this.testLong = testLong;
    this.otherTestClass = otherTestClass;
  }
  public String getTestString() {
    return testString;
  }
  public void setTestString(String testString) {
    this.testString = testString;
  }
  public int getTestInt() {
    return testInt;
  }
  public void setTestInt(int testInt) {
    this.testInt = testInt;
  }
  public long getTestLong() {
    return testLong;
  }
  public void setTestLong(long testLong) {
    this.testLong = testLong;
  }
  public OtherTestClass getOtherTestClass() {
    return otherTestClass;
  }
  public void setOtherTestClass(OtherTestClass otherTestClass) {
    this.otherTestClass = otherTestClass;
  }

}
