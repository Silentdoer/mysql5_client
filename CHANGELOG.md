# Changelog

## 0.1.0

- support dart 2.10.1, and fix some bug, add some example

## 0.2.0
- support dart 2.x

## 0.3.0
- support dart 2.12.0 null-safety
- 迁移方式，优先把基础类先迁移，比如接口/抽象类，然后迁移过程中发现依赖了其他更基础的，则停止迁移当前类而是迁移更基础的
- 对于定义可以用可空类型，比如get A的字段是可空，则A返回值类型也是可空，而方法B返回可空，方法C用了方法B的返回作为返回
- 则方法C的返回值类型也是可空，但是如果是使用B的返回值来赋值或者比较之类的逻辑操作，则直接用!表示不可能是空

## 0.3.1
- dependencies resolve

## 0.3.2
- readme update, fix null type