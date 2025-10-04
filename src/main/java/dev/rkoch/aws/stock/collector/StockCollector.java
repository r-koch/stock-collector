package dev.rkoch.aws.stock.collector;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import javax.naming.LimitExceededException;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import dev.rkoch.aws.collector.utils.State;
import dev.rkoch.aws.s3.parquet.S3Parquet;
import dev.rkoch.aws.stock.collector.api.AlphaVantageApi;
import dev.rkoch.aws.stock.collector.api.NasdaqApi;
import dev.rkoch.aws.stock.collector.exception.NoDataForDateException;
import dev.rkoch.aws.stock.collector.exception.SymbolNotExistsException;
import software.amazon.awssdk.regions.Region;

public class StockCollector {

  private static final String BUCKET_NAME = "dev-rkoch-spre";

  private static final String PARQUET_KEY = "raw/stock/localDate=%s/data.parquet";

  private final LambdaLogger logger;

  private final Region region;

  private AlphaVantageApi alphaVantageApi;

  private NasdaqApi nasdaqApi;

  private S3Parquet s3Parquet;

  private State state;

  public StockCollector(LambdaLogger logger, Region region) {
    this.logger = logger;
    this.region = region;
  }

  public void collect() {
    try {
      collect(Symbols.get());
    } catch (Exception e) {
      logger.log(e.getMessage(), LogLevel.ERROR);
    }
  }

  private void collect(final List<String> symbols) {
    try (State state = getState()) {
      LocalDate limitExceeded = state.getAvLimitExceededDate();
      LocalDate now = LocalDate.now();
      if (limitExceeded == null || now.isAfter(limitExceeded)) {
        LocalDate date = getStartDate();
        for (; date.isBefore(now); date = date.plusDays(1)) {
          try {
            List<StockRecord> records = getData(date, symbols);
            insert(date, records);
            state.setLastAddedStockDate(date);
            logger.log("%s inserted".formatted(date), LogLevel.INFO);
          } catch (NoDataForDateException e) {
            continue;
          } catch (LimitExceededException e) {
            logger.log(e.getMessage(), LogLevel.ERROR);
            state.setAvLimitExceededDate(now);
            return;
          } catch (Exception e) {
            logger.log(e.getMessage(), LogLevel.ERROR);
            return;
          }
        }
      }
    }
  }

  private State getState() {
    if (state == null) {
      state = new State(region, BUCKET_NAME);
    }
    return state;
  }

  private AlphaVantageApi getAlphaVantageApi() {
    if (alphaVantageApi == null) {
      alphaVantageApi = new AlphaVantageApi();
    }
    return alphaVantageApi;
  }

  private List<StockRecord> getData(final LocalDate date, final List<String> symbols) throws LimitExceededException, NoDataForDateException {
    List<StockRecord> records = new ArrayList<>();
    boolean isTradingDay = false;
    for (String symbol : symbols) {
      try {
        try {
          records.add(getNasdaqApi(date).getData(date, symbol));
          isTradingDay = true;
          try {
            Thread.sleep(11L);
          } catch (InterruptedException e) {
            logger.log(e.getMessage(), LogLevel.ERROR);
          }
        } catch (SymbolNotExistsException e) {
          records.add(getAlphaVantageApi().getData(date, symbol));
          isTradingDay = true;
        }
      } catch (NoDataForDateException e) {
        records.add(StockRecord.of(date, symbol, 0, 0, 0, 0, 0));
      }
      if (!isTradingDay) {
        throw new NoDataForDateException();
      }
      logger.log("%s collected %s".formatted(date, symbol), LogLevel.TRACE);
    }
    return records;
  }

  private NasdaqApi getNasdaqApi(final LocalDate date) {
    if (nasdaqApi == null) {
      nasdaqApi = new NasdaqApi(date);
    }
    return nasdaqApi;
  }

  private S3Parquet getS3Parquet() {
    if (s3Parquet == null) {
      s3Parquet = new S3Parquet();
    }
    return s3Parquet;
  }

  private LocalDate getStartDate() {
    LocalDate lastAddedStockDate = getState().getLastAddedStockDate();
    if (lastAddedStockDate == null) {
      LocalDate startDate = LocalDate.now().minusYears(10).minusDays(1);
      getState().setNasdaqStartDate(startDate);
      return startDate;
    } else {
      return lastAddedStockDate.plusDays(1);
    }
  }

  private void insert(final LocalDate date, final List<StockRecord> records) throws Exception {
    getS3Parquet().write(BUCKET_NAME, PARQUET_KEY.formatted(date), records, new StockRecord().getDehydrator());
  }

}
