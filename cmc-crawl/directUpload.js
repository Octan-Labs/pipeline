const fs = require("fs");
const AWS = require("aws-sdk");
if (process.env.NODE_ENV !== "production") require("dotenv").config();

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
