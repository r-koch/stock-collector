package dev.rkoch.aws.stock.collector.api;

import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import javax.naming.LimitExceededException;
import com.crazzyghost.alphavantage.AlphaVantage;
import com.crazzyghost.alphavantage.AlphaVantageException;
import com.crazzyghost.alphavantage.Config;
import com.crazzyghost.alphavantage.parameters.OutputSize;
import com.crazzyghost.alphavantage.timeseries.response.StockUnit;
import com.crazzyghost.alphavantage.timeseries.response.TimeSeriesResponse;
import dev.rkoch.aws.stock.collector.StockRecord;
import dev.rkoch.aws.stock.collector.exception.NoDataForDateException;

public class AlphaVantageApi {

  private static final String ALPHAVANTAGE_API_KEY = "ALPHAVANTAGE_API_KEY";

  private final AlphaVantage alphaVantage;

  private final Map<String, List<StockUnit>> cache = new HashMap<>();

  private final Queue<String> apiKeys = new ArrayDeque<>();

  public AlphaVantageApi() {
    alphaVantage = AlphaVantage.api();
    String apiKey = System.getenv(ALPHAVANTAGE_API_KEY);
    Collections.addAll(apiKeys, apiKey.split(";"));
    setApiKey();
  }

  private void setApiKey() {
    alphaVantage.init(Config.builder().key(apiKeys.poll()).build());
  }

  public StockRecord getData(final LocalDate date, final String symbol) throws LimitExceededException, NoDataForDateException {
    for (StockUnit stockUnit : getStockUnits(symbol)) {
      if (LocalDate.parse(stockUnit.getDate()).isEqual(date)) {
        return StockRecord.of(symbol, stockUnit);
      }
    }
    throw new NoDataForDateException("no data found for %s on date %s".formatted(symbol, date));
  }

  public List<StockRecord> getData(final String symbol) throws LimitExceededException {
    return getStockUnits(symbol).stream().map((stockUnit) -> StockRecord.of(symbol, stockUnit)).toList();
  }

  private List<StockUnit> getStockUnits(final String symbol) throws LimitExceededException {
    List<StockUnit> stockUnits = cache.get(symbol);
    if (stockUnits == null) {
      try {
        String apiSymbol = symbol.replace(".", "-");
        TimeSeriesResponse response = alphaVantage.timeSeries().daily().forSymbol(apiSymbol).outputSize(OutputSize.FULL).fetchSync();
        String errorMessage = response.getErrorMessage();
        if (errorMessage != null && !errorMessage.isBlank()) {
          setApiKey();
          return getStockUnits(symbol);
        }
        stockUnits = response.getStockUnits();
        cache.put(symbol, stockUnits);
      } catch (AlphaVantageException e) {
        if ("API Key not set".equalsIgnoreCase(e.getMessage())) {
          throw new LimitExceededException("alphavantage limit exceeded for %s".formatted(LocalDate.now()));
        } else {
          throw e;
        }
      }
    }
    return stockUnits;
  }

}
