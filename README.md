
Generate database metadata json file:

```sh
mvn compile exec:java -Pgen-dbmd "-Ddbmd.file=$HOME/tmp/dbmd.json" "-Ddb.props=$HOME/tmp/jdbc-localdev.props"
```

Generate table type record definitions:

```sh
mvn compile exec:java  -Pgen-table-types "-Ddbmd.file=$HOME/tmp/dbmd.json" "-Djava.base.dir=$HOME/tmp" "-Djava.package=org.myapp"
```

Start `jshell` with project classpath:

```sh
mvn compile jshell:run
```