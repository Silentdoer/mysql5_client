// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

/// The mysql_client library.
library mysql_client;

export 'package:mysql_client/src/connection.dart'
    show
        ConnectionFactory,
        Connection,
        QueryResult,
        PreparedStatement,
        ColumnDefinition,
        CommandResult,
        DataIterator,
        ConnectionError,
        QueryError,
        PreparedStatementError;
