package info.jerrinot.jetcustomsink;

import java.io.Serializable;

public final class StockPriceUpdate implements Serializable {
    private final String symbol;
    private final long price;
    private final long timestamp;

    public StockPriceUpdate(String symbol, long price, long timestamp) {
        this.symbol = symbol;
        this.price = price;
        this.timestamp = timestamp;
    }

    public long getPrice() {
        return price;
    }

    public String getSymbol() {
        return symbol;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "StockPriceUpdate{" +
                "symbol='" + symbol + '\'' +
                ", price=" + price +
                ", timestamp=" + timestamp +
                '}';
    }
}
