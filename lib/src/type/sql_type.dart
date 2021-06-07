import 'package:decimal/decimal.dart';

// 这里所有的复杂类型都先以String的方式写入，比如DateTime和Decimal
// 以DateTime的类型但是值却是'2021-08-33 04:04:33'这种格式会报错，估计要以字节写入
// 但是没有相关的资料，所以先不搞

// tinyint is also bool
const int MYSQL_TYPE_TINY = 0x01;
const int MYSQL_TYPE_LONG = 0x03;
const int MYSQL_TYPE_DOUBLE = 0x05;
const int MYSQL_TYPE_NULL = 0x06;
const int MYSQL_TYPE_TIMESTAMP = 0x07;
const int MYSQL_TYPE_LONGLONG = 0x08;
const int MYSQL_TYPE_DATETIME = 0x0c;
const int MYSQL_TYPE_VAR_STRING = 0xfd;
// decimal
const int MYSQL_TYPE_NEWDECIMAL = 0xf6;

enum SqlType {
  TINY,
  LONG,
  DOUBLE,
  NULL,
  TIMESTAMP,
  LONGLONG,
  DATETIME,
  VAR_STRING,
  DECIMAL,
}

// FIXME
int getMysqlTypeFlagFromSqlType(SqlType sqlType) {
  switch (sqlType) {
    case SqlType.DOUBLE:
      return MYSQL_TYPE_DOUBLE;
    case SqlType.LONG:
      return MYSQL_TYPE_LONG;
    case SqlType.LONGLONG:
      return MYSQL_TYPE_LONGLONG;
    case SqlType.NULL:
      return MYSQL_TYPE_NULL;
    case SqlType.TIMESTAMP:
      return MYSQL_TYPE_VAR_STRING;
    case SqlType.DATETIME:
      return MYSQL_TYPE_VAR_STRING;
    case SqlType.TINY:
      return MYSQL_TYPE_TINY;
    case SqlType.VAR_STRING:
      return MYSQL_TYPE_VAR_STRING;
    case SqlType.DECIMAL:
      return MYSQL_TYPE_VAR_STRING;
    // TODO 有新类型来了这里需要修改
  }
}

// FIXME，先不动，似乎读取row时就算有mysql类型信息还是能以String方式读取出来
SqlType getSqlTypeFromMysqlType(int mysqlTypeFlag) {
  switch (mysqlTypeFlag) {
    case MYSQL_TYPE_NULL:
      return SqlType.NULL;
    case MYSQL_TYPE_VAR_STRING:
      return SqlType.VAR_STRING;
    case MYSQL_TYPE_LONG:
      return SqlType.LONG;
    case MYSQL_TYPE_LONGLONG:
      return SqlType.LONGLONG;
    case MYSQL_TYPE_DOUBLE:
      return SqlType.DOUBLE;
    case MYSQL_TYPE_TINY:
      return SqlType.TINY;
    case MYSQL_TYPE_DATETIME:
      return SqlType.DATETIME;
    case MYSQL_TYPE_TIMESTAMP:
      return SqlType.TIMESTAMP;
    case MYSQL_TYPE_NEWDECIMAL:
      return SqlType.DECIMAL;
    default:
      throw new UnsupportedError(
          "MySql type flag not supported $mysqlTypeFlag");
  }
}

// FIXME
int getSqlTypeFromValue(dynamic value) {
  if (value == null) {
    return MYSQL_TYPE_NULL;
  } else if (value is String) {
    return MYSQL_TYPE_VAR_STRING;
  } /* 数据库的bigint其实就是long，即8 byte */ else if (value is int) {
    // bigint
    return MYSQL_TYPE_LONGLONG;
  } else if (value is double) {
    return MYSQL_TYPE_DOUBLE;
  } else if (value is Decimal) {
    return MYSQL_TYPE_VAR_STRING;
  } else if (value is bool) {
    return MYSQL_TYPE_TINY;
  } else if (value is DateTime) {
    return MYSQL_TYPE_VAR_STRING;
  } else {
    throw new UnsupportedError("Value type not supported ${value.runtimeType}");
  }
}
