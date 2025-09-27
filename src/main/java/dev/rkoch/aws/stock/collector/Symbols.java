package dev.rkoch.aws.stock.collector;

import java.util.ArrayList;
import java.util.List;
import dev.rkoch.aws.utils.ddb.DDb;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

public enum Symbols {

  INSTANCE;

  private static final int INITIAL_CAPACITY = 503;

  private static final List<String> SYMBOLS = new ArrayList<>(INITIAL_CAPACITY);

  public static List<String> get() {
    if (SYMBOLS.isEmpty()) {
      ScanRequest request = ScanRequest.builder().tableName("STOCK_SYMBOL").build();
      ScanResponse response = DDb.client().scan(request);
      SYMBOLS.addAll(response.items().stream().map((item) -> item.get("symbol").s()).sorted().toList());
    }
    return SYMBOLS;
  }

  public static List<String> getAfter(final String fromSymbolAfter) {
    List<String> symbols = get();
    if (fromSymbolAfter != null && !fromSymbolAfter.isBlank()) {
      int indexOf = symbols.indexOf(fromSymbolAfter);
      if (indexOf != -1) {
        return symbols.subList(indexOf + 1, symbols.size());
      }
    }
    return symbols;
  }

  public static String getLast() {
    return get().getLast();
  }

}
