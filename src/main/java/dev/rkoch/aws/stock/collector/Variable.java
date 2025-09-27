package dev.rkoch.aws.stock.collector;

import dev.rkoch.aws.utils.ddb.DefaultVariable;

public enum Variable implements DefaultVariable {

  DATE_LIMIT_EXCEEDED_AV, //
  LAST_ADDED_STOCK, //
  DATE_LAST_ADDED_STOCK, //
  ;

  @Override
  public String getTable() {
    return "STATE";
  }

}
