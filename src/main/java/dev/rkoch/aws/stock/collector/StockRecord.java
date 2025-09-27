package dev.rkoch.aws.stock.collector;

import java.time.LocalDate;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import com.crazzyghost.alphavantage.timeseries.response.StockUnit;
import blue.strategic.parquet.Dehydrator;
import dev.rkoch.aws.s3.parquet.ParquetRecord;

public class StockRecord implements ParquetRecord {

  public static StockRecord of(final LocalDate localDate, final String id, final double close, final double high, final double low, final double open,
      final long volume) {
    return new StockRecord(localDate, id, close, high, low, open, volume);
  }

  public static StockRecord of(final LocalDate localDate, final String id, final String close, final String high, final String low, final String open,
      final String volume) {
    double c = Double.parseDouble(close);
    double h = Double.parseDouble(high);
    double l = Double.parseDouble(low);
    double o = Double.parseDouble(open);
    long v = parseLong(volume);
    return new StockRecord(localDate, id, c, h, l, o, v);
  }

  public static StockRecord of(final String id, final StockUnit stockUnit) {
    return new StockRecord(LocalDate.parse(stockUnit.getDate()), id, stockUnit.getClose(), stockUnit.getHigh(), stockUnit.getLow(), stockUnit.getOpen(),
        stockUnit.getVolume());
  }

  private static long parseLong(final String value) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      System.out.println(e.getMessage());
      return 0L;
    }
  }

  private final LocalDate localDate;
  private final String id;
  private final double close;
  private final double high;
  private final double low;
  private final double open;
  private final long volume;

  public StockRecord(LocalDate localDate, String id, double close, double high, double low, double open, long volume) {
    this.localDate = localDate;
    this.id = id;
    this.close = close;
    this.high = high;
    this.low = low;
    this.open = open;
    this.volume = volume;
  }

  public double getClose() {
    return close;
  }

  @Override
  public Dehydrator<ParquetRecord> getDehydrator() {
    return (record, valueWriter) -> {
      valueWriter.write("localDate", localDate.toEpochDay());
      valueWriter.write("id", id);
      valueWriter.write("close", close);
      valueWriter.write("high", high);
      valueWriter.write("low", low);
      valueWriter.write("open", open);
      valueWriter.write("volume", volume);
    };
  }

  public double getHigh() {
    return high;
  }

  public String getId() {
    return id;
  }

  public LocalDate getLocalDate() {
    return localDate;
  }

  public double getLow() {
    return low;
  }

  public double getOpen() {
    return open;
  }

  @Override
  public MessageType getSchema() {
    return new MessageType("stock-record", //
        Types.required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named("localDate"), //
        Types.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("id"), //
        Types.required(PrimitiveTypeName.DOUBLE).named("close"), //
        Types.required(PrimitiveTypeName.DOUBLE).named("high"), //
        Types.required(PrimitiveTypeName.DOUBLE).named("low"), //
        Types.required(PrimitiveTypeName.DOUBLE).named("open"), //
        Types.required(PrimitiveTypeName.INT64).named("volume") //
    );
  }

  public long getVolume() {
    return volume;
  }

}
