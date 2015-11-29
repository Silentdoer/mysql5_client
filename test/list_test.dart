// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:io";
import "dart:async";
import "dart:typed_data";
import "dart:collection";

// sudo ngrep -x -q -d lo0 '' 'port 3306'

const int ITERATION = 1000000;

Future main() async {

  var list = new List.generate(100000, (i) => i, growable: false);
  var list2 = new Uint8List.fromList(list);

  // testare getRange con addAll
  var p = 50000;
  var l = 5;

  var sw = new Stopwatch();
  sw.start();
  for(var i = 0; i < ITERATION; i++) {
    [].addAll(list.sublist(p, p + l));
  }
  print("Elapsed in ${sw.elapsedMilliseconds} ms");
  sw.reset();
  for(var i = 0; i < ITERATION; i++) {
    var r = new List(l);
    for (var p1 = 0, p2 = p; p1 < l; p1++, p2++) {
      r[p1] = list[p2];
    }
  }
  print("Elapsed in ${sw.elapsedMilliseconds} ms");
}