package com.zzzzzzzs.sqllineage.controller;

import com.zzzzzzzs.sqllineage.core.ParseSql;
import org.apache.calcite.sql.parser.SqlParseException;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/parse")
public class ParseController {
  ParseSql parseSql = new ParseSql();

  // parse select
  @RequestMapping(value = "/columnsLineage", method = RequestMethod.GET)
  public String parseSelect(String sql) throws SqlParseException {
    String res = parseSql.parseSelect(sql);

    return res;
  }
}
