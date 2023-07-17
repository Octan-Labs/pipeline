const cryptocurrencies = require("./top1000.json").data;
const axios = require("axios");
const fs = require("fs");
const AWS = require("aws-sdk");
if (process.env.NODE_ENV !== "production") require("dotenv").config();

function formatTimestamp(timestamp) {
  return Math.floor(new Date(timestamp).getTime() / 1000);
}

async function directUpload() {
  const timeStart = process.env.START_TIME;
  const timeEnd = process.env.END_TIME;
  const BUCKET = process.env.AWS_S3_BUCKET;
  const REGION = process.env.S3_REGION;
  const ACCESS_KEY = process.env.AWS_ACCESS_KEY_ID;
  const SECRET_KEY = process.env.AWS_SECRET_ACCESS_KEY;

  const localFile = `./cmc_historical/cmc_historical_price_top1000_${timeStart}_${timeEnd}.csv`;
  const fileRemoteName = `cmc_historical_price_top1000_${timeStart}_${timeEnd}.csv`;

  AWS.config.update({
    accessKeyId: ACCESS_KEY,
    secretAccessKey: SECRET_KEY,
    region: REGION,
  });

  var s3 = new AWS.S3();

  s3.putObject({
    Bucket: BUCKET,
    Body: fs.readFileSync(localFile),
    Key: fileRemoteName,
  })
    .promise()
    .then((res) => {
      console.log(`Upload succeeded - `, res);
    })
    .catch((err) => {
      console.log("Upload failed:", err);
    });
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
  await directUpload();
}

return main().catch((err) => {
  console.log(err);
  setTimeout(() => {
    return main();
  }, 5000);
});
