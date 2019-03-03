package freynder.kafka;

public class OtherTestClass {
  private String[] Results;
  private byte[] testBytes;
  public OtherTestClass(String[] results, byte[] testBytes) {
    super();
    Results = results;
    this.testBytes = testBytes;
  }
  public String[] getResults() {
    return Results;
  }
  public void setResults(String[] results) {
    Results = results;
  }
  public byte[] getTestBytes() {
    return testBytes;
  }
  public void setTestBytes(byte[] testBytes) {
    this.testBytes = testBytes;
  }
  
}
