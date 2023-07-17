const cryptocurrencies = require("./top1000.json").data;
const axios = require("axios");
const fs = require("fs");
if (process.env.NODE_ENV !== "production") require("dotenv").config();

function formatTimestamp(timestamp) {
  return Math.floor(new Date(timestamp).getTime() / 1000);
}

async function main() {
  const instance = axios.create({
    baseURL: "https://api.coinmarketcap.com/data-api/v3/cryptocurrency",
  });

  const timeStart = process.env.START_TIME;
  const timeEnd = process.env.END_TIME;

  const writeStream = fs.WriteStream(
    `./cmc_historical/cmc_historical_price_top1000_${timeStart}_${timeEnd}.csv`
  );

  if (!timeStart || !timeEnd) {
    console.log("Config env first");
    process.exit(1);
  }

  for (const cryptocurrency of cryptocurrencies) {
    const id = cryptocurrency.id,
      rank = cryptocurrency.rank,
      name = cryptocurrency.name,
      symbol = cryptocurrency.symbol;

    // id,rank,name,symbol,open,high,low,close,volume,marketCap,timestamp
    const res = await instance.get(
      `/historical?id=${id}&convertId=2781&timeStart=${timeStart}&timeEnd=${timeEnd}`
    );
    const quotes = res.data.data.quotes;

    for (const { quote } of quotes) {
      const row = [
        id,
        rank,
        name,
        symbol,
        quote.open,
        quote.high,
        quote.low,
        quote.close,
        quote.volume,
        quote.marketCap,
        formatTimestamp(quote.timestamp),
      ].toString();

      writeStream.write(row + "\r\n");
    }
  }
  writeStream.end();
}

main().catch(console.log);
