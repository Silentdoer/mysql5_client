import "dart:async";

main() async {
  var sw = new Stopwatch()..start();
  for (var i = 0; i < 1000000; i++) {
    var value = test();
    value = value is Future ? await value : value;
  }
  print(sw.elapsedMilliseconds);
  sw = new Stopwatch()..start();
  for (var i = 0; i < 1000000; i++) {
    var value = await test();
  }
  print(sw.elapsedMilliseconds);
}

test() {
}