package com.zzzzzzzs.sqllineage.core;

import cn.hutool.core.io.file.FileReader;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;


import java.util.List;

public class Parse {
  public static void main(String[] args)  {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config =
        Frameworks.newConfigBuilder()
            .parserConfig(
                SqlParser.configBuilder()
                    .setParserFactory(SqlParserImpl.FACTORY)
                    .setCaseSensitive(false)
                    .setQuoting(Quoting.BACK_TICK)
                    .setQuotedCasing(Casing.TO_UPPER)
                    .setUnquotedCasing(Casing.TO_UPPER)
                    .setConformance(SqlConformanceEnum.DEFAULT)
                    .build())
            .build();
    FileReader fileReader = new FileReader("./sql/query01.sql");
    String sql = fileReader.readString().trim();
    if (sql.endsWith(";")) {
      sql = sql.substring(0, sql.length() - 1);
    }
    SqlParser parser = SqlParser.create(sql, config.getParserConfig());
    try {
      SqlNode sqlNode = parser.parseStmt();
      // 然后解析出来的sqlNode中的属性就可以了
      System.out.println(sqlNode.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
