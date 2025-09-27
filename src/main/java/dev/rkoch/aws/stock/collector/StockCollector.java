package dev.rkoch.aws.stock.collector;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import javax.naming.LimitExceededException;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import dev.rkoch.aws.s3.parquet.S3Parquet;
import dev.rkoch.aws.stock.collector.api.AlphaVantageApi;
import dev.rkoch.aws.stock.collector.api.NasdaqApi;
import dev.rkoch.aws.stock.collector.exception.NoDataForDateException;
import dev.rkoch.aws.stock.collector.exception.SymbolNotExistsException;
import dev.rkoch.aws.utils.log.SystemOutLambdaLogger;

public class StockCollector {

  private static final String BUCKET_NAME = "dev-rkoch-spre";

  private static final String PARQUET_KEY = "raw/stock/localDate=%s/data.parquet";

  public static void main(String[] args) {
    new StockCollector(new SystemOutLambdaLogger()).collect();
  }

  private final LambdaLogger logger;

  private AlphaVantageApi alphaVantageApi;

  private NasdaqApi nasdaqApi;

  private S3Parquet s3Parquet;

  public StockCollector(LambdaLogger logger) {
    this.logger = logger;
  }

  public void collect() {
    collect(Symbols.get());
  }

  private void collect(final List<String> symbols) {
    LocalDate limitExceeded = LocalDate.parse(Variable.DATE_LIMIT_EXCEEDED_AV.get());
    LocalDate now = LocalDate.now();
    if (now.isAfter(limitExceeded)) {
      LocalDate date = getStartDate();
      for (; date.isBefore(now); date = date.plusDays(1)) {
        try {
          List<StockRecord> records = getData(date, symbols);
          insert(date, records);
          Variable.DATE_LAST_ADDED_STOCK.set(date.toString());
          logger.log("%s inserted".formatted(date), LogLevel.INFO);
        } catch (LimitExceededException e) {
          logger.log(e.getMessage(), LogLevel.ERROR);
          Variable.DATE_LIMIT_EXCEEDED_AV.set(now.toString());
          return;
        } catch (NoDataForDateException e) {
          continue;
        } catch (Exception e) {
          logger.log(e.getMessage(), LogLevel.ERROR);
          return;
        }
      }
    }
  }

  private AlphaVantageApi getAlphaVantageApi() {
    if (alphaVantageApi == null) {
      alphaVantageApi = new AlphaVantageApi();
    }
    return alphaVantageApi;
  }

  private List<StockRecord> getData(final LocalDate date, final List<String> symbols) throws LimitExceededException, NoDataForDateException {
    List<StockRecord> records = new ArrayList<>();
    for (String symbol : symbols) {
      try {
        records.add(getNasdaqApi().getData(date, symbol));
      } catch (SymbolNotExistsException e) {
        records.add(getAlphaVantageApi().getData(date, symbol));
      }
      logger.log("%s collected %s".formatted(date, symbol), LogLevel.TRACE);
      try {
        Thread.sleep(11L);
      } catch (InterruptedException e) {
        logger.log(e.getMessage(), LogLevel.ERROR);
      }
    }
    return records;
  }

  private NasdaqApi getNasdaqApi() {
    if (nasdaqApi == null) {
      nasdaqApi = new NasdaqApi();
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
    String lastAdded = Variable.DATE_LAST_ADDED_STOCK.get();
    if (lastAdded.isBlank()) {
      return LocalDate.now().minusYears(10).minusDays(1);
    } else {
      return LocalDate.parse(lastAdded).plusDays(1);
    }
  }

  private void insert(final LocalDate date, final List<StockRecord> records) throws Exception {
    getS3Parquet().write(BUCKET_NAME, PARQUET_KEY.formatted(date), records);
  }

}
