package freynder.kafka;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class GeneratorTest {

  @Test
  void test() {
    //TestClass tc = new TestClass("Test", 1, 2, new OtherTestClass(new String[] {"test1", "test2"}, null));
    Generator g = new Generator();
    String results = g.generate(TestClass.class);
    System.out.println(results);
    //fail("Not yet implemented");
  }

}
