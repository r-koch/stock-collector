package dev.rkoch.aws.stock.collector.api;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.naming.LimitExceededException;
import com.crazzyghost.alphavantage.AlphaVantage;
import com.crazzyghost.alphavantage.Config;
import com.crazzyghost.alphavantage.parameters.OutputSize;
import com.crazzyghost.alphavantage.timeseries.response.StockUnit;
import com.crazzyghost.alphavantage.timeseries.response.TimeSeriesResponse;
import dev.rkoch.aws.stock.collector.StockRecord;

public class AlphaVantageApi {

  private static final String ALPHAVANTAGE_API_KEY = "ALPHAVANTAGE_API_KEY";

  private final AlphaVantage alphaVantage;

  private final Map<String, List<StockUnit>> cache = new HashMap<>();

  public AlphaVantageApi() {
    alphaVantage = AlphaVantage.api();
    alphaVantage.init(Config.builder().key(System.getenv(ALPHAVANTAGE_API_KEY)).build());
  }

  public StockRecord getData(final LocalDate date, final String symbol) throws LimitExceededException {
    List<StockUnit> stockUnits = cache.get(symbol);
    if (stockUnits == null) {
      String apiSymbol = symbol.replace(".", "-");
      TimeSeriesResponse response = alphaVantage.timeSeries().daily().forSymbol(apiSymbol).outputSize(OutputSize.FULL).fetchSync();
      String errorMessage = response.getErrorMessage();
      if (errorMessage != null && !errorMessage.isBlank()) {
        throw new LimitExceededException("alphavantage limit exceeded for %s".formatted(date));
      }
      stockUnits = response.getStockUnits();
      cache.put(symbol, stockUnits);
    }
    for (StockUnit stockUnit : stockUnits) {
      if (LocalDate.parse(stockUnit.getDate()).isEqual(date)) {
        String close = cleanNumber(stockUnit.getClose());
        String high = cleanNumber(stockUnit.getHigh());
        String low = cleanNumber(stockUnit.getLow());
        String open = cleanNumber(stockUnit.getOpen());
        String volume = cleanNumber(stockUnit.getVolume());
        return StockRecord.of(date, symbol, close, high, low, open, volume);
      }
    }
    throw new RuntimeException("no data found for %s on date %s".formatted(symbol, date.toString()));
  }

  private String cleanNumber(final double value) {
    String v = String.format(Locale.US, "%.4f", value);
    v = v.replaceAll("0*$", "");
    v = v.replaceAll("\\.$", "");
    return v;
  }

}
