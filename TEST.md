You can use Docker to create a temporary mysql instance for testing:

```
docker run --name mysql5_test -d -P \
   -e MYSQL_ROOT_PASSWORD=root \
   -e MYSQL_USER=test \
   -e MYSQL_PASSWORD=test \
   -e MYSQL_DATABASE=test \
   mysql
```

This will create and run a Docker container with the name `mysql5_test`.

You can determine the exposed port by running

```
docker port mysql5_test
```

*Note: depending on your Docker configuration, you will likely have to change
the host IP address.*