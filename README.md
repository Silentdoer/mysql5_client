# mysql_client

A mysql5.x library for Dart developers.

## Usage

A simple usage example:

    import '../lib/mysql5_client.dart';

    void main() async {
      Connection? connection;

      try {
        connection = await new ConnectionFactory()
            .connect("localhost", 3306, "root", "shit", "db_test");

        // please pre-insert row that id = 1
        var statement = await connection.prepareQuery("UPDATE stud SET name = ? WHERE id = 1");
        statement.setParameter(0, "fuck老天");
        var queryResult = await statement.executeQuery();
        print(queryResult.affectedRows);

        queryResult =
            await connection.executeQuery("SELECT * FROM stud WHERE id = 1");

        // rows
        while (true) {
          var next = await queryResult.next();
          if (!next) {
            break;
          }

          print("${queryResult.getNumValue(0)} : ${queryResult.getStringValue(1)}}");
        }
      } finally {
        await connection?.close();
      }
    }

## Features and bugs

Please file feature requests and bugs at the [issue tracker][tracker].

[tracker]: https://github.com/Silentdoer/mysql5_client