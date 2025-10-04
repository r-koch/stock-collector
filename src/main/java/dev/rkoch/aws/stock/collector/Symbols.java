package dev.rkoch.aws.stock.collector;

import java.util.ArrayList;
import java.util.List;
import dev.rkoch.aws.s3.parquet.S3Parquet;

public enum Symbols {

  INSTANCE;

  private static final String BUCKET_NAME = "dev-rkoch-spre";

  private static final String PARQUET_KEY = "symbols/spx.parquet";

  private static final int INITIAL_CAPACITY = 503;

  private static final List<String> SYMBOLS = new ArrayList<>(INITIAL_CAPACITY);

  private static final List<SymbolRecord> RECORDS = new ArrayList<>(INITIAL_CAPACITY);

  public static List<String> get() {
    if (SYMBOLS.isEmpty()) {
      SYMBOLS.addAll(getRecords().stream().map((record) -> record.getId()).toList());
    }
    return SYMBOLS;
  }

  public static List<SymbolRecord> getRecords() {
    if (RECORDS.isEmpty()) {
      try {
        RECORDS.addAll(new S3Parquet().read(BUCKET_NAME, PARQUET_KEY, new SymbolRecord().getHydrator()));
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
    return RECORDS;
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

  public static String getLast() throws Exception {
    return get().getLast();
  }

  public static void setRecords(List<SymbolRecord> records) throws Exception {
    new S3Parquet().write(BUCKET_NAME, PARQUET_KEY, records, new SymbolRecord().getDehydrator());
  }

  public static void main(String[] args) throws Exception {
    // ScanRequest request = ScanRequest.builder().tableName("STOCK_SYMBOL").build();
    // ScanResponse response = DDb.client().scan(request);
    //
    // List<SymbolRecord> records = new ArrayList<>();
    // for (Map<String, AttributeValue> item : response.items()) {
    // records.add(new SymbolRecord(item.get("symbol").s(), item.get("sector").s()));
    // }
    //

    // List<String> after = getAfter("azo");
    // System.out.println(after);

    setRecords(getRecords().stream().sorted((r1, r2) -> r1.getId().compareTo(r2.getId())).toList());

    get().stream().forEach(System.out::println);
  }

}
