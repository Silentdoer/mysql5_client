// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_commons;

// TODO spostarli nel protocollo se possibile

const int NULL_TERMINATOR = 0x00;

const int PREFIX_NULL = 0xfb;
const int PREFIX_UNDEFINED = 0xff;
const int PREFIX_INT_2 = 0xfc;
const int PREFIX_INT_3 = 0xfd;
const int PREFIX_INT_8 = 0xfe;

const int MAX_INT_1 = 0xfb;
const int MAX_INT_2 = 2 << (2 * 8 - 1);
const int MAX_INT_3 = 2 << (3 * 8 - 1);
const int MAX_INT_8 = 2 << (8 * 8 - 1);