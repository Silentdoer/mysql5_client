void main() {
  try {
    test1();
    print("OK - should never go here"); // should never go here
  } catch (e) {
    print("ERROR: $e");
  }

  try {
    test2();
    print("OK - should never go here"); // should never go here
  } catch (e) {
    print("ERROR: $e");
  }
}

void test1() {
  try {
    throw new ArgumentError("error");
    print("OK 2"); // should never go here
  } finally {
    try {
      throw new ArgumentError("error 2");
    } catch(e) {
      print("error 2 catched");
    }
  }
}

void test2() {
  try {
    throwError(); // call a method that throws an error
    print("OK 2"); // should never go here
  } finally {
    try {
      throw new ArgumentError("error 2");
    } catch(e) {
      print("error 2 catched");
    }
  }
}

void throwError() {
  throw new ArgumentError("error");
}