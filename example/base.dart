import '../lib/mysql5_client.dart';

void main() async {
  Connection? connection;

  try {
    connection = await new ConnectionFactory()
        .connect("localhost", 3306, "root", "wyzpass", "db_test");
    // ConnectionImpl
    print(connection.runtimeType);
    var queryResult;
    var statement;

    statement =
        await connection.prepareQuery("UPDATE stud SET name = ? WHERE id = 1");

    statement.setParameter(0, "fuck老天");

    queryResult = await statement.executeQuery();

    print(queryResult.affectedRows);

    statement.setParameter(0, "大地啊88");

    queryResult = await statement.executeQuery();

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
