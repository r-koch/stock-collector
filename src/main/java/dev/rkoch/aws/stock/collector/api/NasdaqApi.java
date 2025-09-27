package dev.rkoch.aws.stock.collector.api;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.rkoch.aws.stock.collector.StockRecord;
import dev.rkoch.aws.stock.collector.exception.NoDataForDateException;
import dev.rkoch.aws.stock.collector.exception.SymbolNotExistsException;

public class NasdaqApi {

  // https://api.nasdaq.com/api/quote/tsla/historical?assetclass=stocks&fromdate=2025-08-07&limit=1&todate=2025-08-08
  // https://api.nasdaq.com/api/quote/tsla/historical?assetclass=stocks&limit=10000&fromdate=1999-11-01&todate=2025-09-07
  private static final String API_URL = "https://api.nasdaq.com/api/quote/%s/historical?assetclass=stocks&limit=10000&fromdate=%s&todate=%s";

  private static final DateTimeFormatter MM_DD_YYYY = DateTimeFormatter.ofPattern("MM/dd/uuuu");

  private final HttpClient httpClient = HttpClient.newHttpClient();

  private final ObjectMapper objectMapper = new ObjectMapper();

  public StockRecord getData(final LocalDate date, final String symbol) throws NoDataForDateException, SymbolNotExistsException {
    try {
      HttpRequest httpRequest = HttpRequest.newBuilder(getUri(date, symbol)).build();
      HttpResponse<String> httpResponse = httpClient.send(httpRequest, BodyHandlers.ofString());
      String body = httpResponse.body();
      JsonNode tree = objectMapper.readTree(body);
      JsonNode code = tree.findValue("code");
      if (code != null && code.asInt() == 1001) {
        throw new SymbolNotExistsException();
      }
      JsonNode rows = tree.findValue("rows");
      if (rows == null) {
        throw new NoDataForDateException(date);
      }
      for (JsonNode row : rows) {
        LocalDate rowDate = LocalDate.parse(row.findValue("date").asText(), MM_DD_YYYY);
        if (rowDate.isEqual(date)) {
          String close = cleanNumber(row.findValue("close").asText());
          String high = cleanNumber(row.findValue("high").asText());
          String low = cleanNumber(row.findValue("low").asText());
          String open = cleanNumber(row.findValue("open").asText());
          String volume = cleanNumber(row.findValue("volume").asText());

          return StockRecord.of(date, symbol, close, high, low, open, volume);
        }
      }
      throw new NoDataForDateException(date);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static String cleanNumber(final String number) {
    return number.replace("$", "").replace(",", "");
  }

  private static URI getUri(final LocalDate date, final String symbol) {
    String apiSymbol = symbol.replace(".", "-");
    String fromDate = date.minusDays(5).toString();
    String toDate = date.toString();
    return URI.create(API_URL.formatted(apiSymbol, fromDate, toDate));
  }

}
