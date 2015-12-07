// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_commons;

import "dart:math";

const int NULL_TERMINATOR = 0x00;

const int PREFIX_NULL = 0xfb;
const int PREFIX_UNDEFINED = 0xff;

const int MAX_INT_1 = 0xfb;
final int MAX_INT_2 = pow(2, 2 * 8);
const int PREFIX_INT_2 = 0xfc;
final int MAX_INT_3 = pow(2, 3 * 8);
const int PREFIX_INT_3 = 0xfd;
final int MAX_INT_8 = pow(2, 8 * 8);
const int PREFIX_INT_8 = 0xfe;