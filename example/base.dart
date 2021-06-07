import 'package:decimal/decimal.dart';

import '../lib/mysql5_client.dart';

void main() async {
  Connection? connection;

  try {
    connection = await new ConnectionFactory()
        .connect("localhost", 3306, "root", "wyzpass", "db_test");

    // ConnectionImpl
    print(connection.runtimeType);

    var statement =
        await connection.prepareQuery("UPDATE stud SET name = ? WHERE id = 1");
    statement.setParameter(0, "fuck33老天");
    var queryResult = await statement.executeQuery();
    // 值相同是不会生效的；
    print(queryResult.affectedRows);
    // null值可以被正确识别，包括select也是一样，目前还没搞懂
    statement.setParameter(0, null);
    queryResult = await statement.executeQuery();
    // 值相同是不会生效的；
    print(queryResult.affectedRows);

    statement =
        await connection.prepareQuery("UPDATE stud SET prop1 = ? WHERE id = 1");
    statement.setParameter(0, 999999999999);
    queryResult = await statement.executeQuery();
    print(queryResult.affectedRows);

    // text类型也可以被正确处理，那类型分那么细的作用是什么？
    statement =
        await connection.prepareQuery("UPDATE stud SET prop2 = ? WHERE id = 1");
    // mysql里类型划分不明确，所以这里的值可以是数字【也就是说发送给mysql服务的其实就是字符串而已】
    // 发送中的MYSQL_TYPE_XXX这种并没有啥软用貌似【但是又确实同时发送给了mysql，mysql自动忽略了？】
    // ok
    //statement.setParameter(0, 999999999999);
    statement.setParameter(0, '这个是text时间分开了');
    queryResult = await statement.executeQuery();
    print(queryResult.affectedRows);

    statement =
        await connection.prepareQuery("UPDATE stud SET prop3 = ? WHERE id = 1");
    // ok，其实本质上应该是var_string，DateTime是我们程序里进行了处理转换为了var_string
    //statement.setParameter(0, '2020-04-08 08:33:32');
    statement.setParameter(0, DateTime.now());
    queryResult = await statement.executeQuery();
    print(queryResult.affectedRows);

    statement =
        await connection.prepareQuery("UPDATE stud SET prop4 = ? WHERE id = 1");
    // ok，其实本质上应该是var_string，Decimal是我们程序里进行了处理转换为了var_string
    //statement.setParameter(0, '89933.83');
    statement.setParameter(0, Decimal.parse('23.889'));
    queryResult = await statement.executeQuery();
    print(queryResult.affectedRows);

    statement =
        await connection.prepareQuery("UPDATE stud SET prop5 = ? WHERE id = 1");
    // ok，其实本质上应该是var_string，Decimal是我们程序里进行了处理转换为了var_string
    statement.setParameter(0, '8933.83');
    //statement.setParameter(0, 99.334);
    //statement.setParameter(0, 96);
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

      print("${queryResult.getNumValue(0)} : ${queryResult.getStringValue(1)}");
      // num也可以用String类型来读取
      print(
          "${queryResult.getStringValue(2)} : ${queryResult.getStringValue(3)}");
      print(
          "${queryResult.getStringValue(4)} : ${queryResult.getStringValue(5)}");
      print(
          "${queryResult.getStringValue(6)} : ${queryResult.getStringValue(6)}");
      print(
          "${queryResult.getDateTimeValue(4)} : ${queryResult.getDecimalValue(5)}");
      print(
          "${queryResult.getIntegerValue(0)} : ${queryResult.getDoubleValue(6)}");
    }
  } finally {
    await connection?.close();
  }
}
