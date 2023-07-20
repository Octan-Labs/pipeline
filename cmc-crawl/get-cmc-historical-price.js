const topCMC = require("./cmc_top.json");
const axios = require("axios");
const fs = require("fs");
const AWS = require("aws-sdk");
require("dotenv").config();

const timeStart = process.env.START_TIME;
const timeEnd = process.env.END_TIME;
const BUCKET = process.env.AWS_S3_BUCKET;
const REGION = process.env.S3_REGION;
const ACCESS_KEY = process.env.AWS_ACCESS_KEY_ID;
const SECRET_KEY = process.env.AWS_SECRET_ACCESS_KEY;

if (!timeStart || !timeEnd) {
	console.log("TIME ENV is required");
	process.exit(1);
}

if (!BUCKET || !REGION || !ACCESS_KEY || !SECRET_KEY) {
	console.log("AWS ENV is required");
	process.exit(1);
}

function formatTimestamp(timestamp) {
	return Math.floor(new Date(timestamp).getTime() / 1000);
}

const filename = `cmc_historical_price_${timeStart}_${timeEnd}.csv`;

const instance = axios.create({
	baseURL: "https://api.coinmarketcap.com/data-api/v3/cryptocurrency",
});

const writeStream = fs.WriteStream(`./${filename}`);

function delay(t, v) {
	return new Promise((resolve) => setTimeout(resolve, t, v));
}

async function uploadToS3() {
	AWS.config.update({
		accessKeyId: ACCESS_KEY,
		secretAccessKey: SECRET_KEY,
		region: REGION,
	});
	var s3 = new AWS.S3();

	console.log("Upload file to S3");

	s3.putObject({
		Bucket: BUCKET,
		Body: fs.readFileSync(`./${filename}`),
		Key: filename,
	})
		.promise()
		.then((res) => {
			console.log(`Upload succeeded - `, res);
		})
		.catch((err) => {
			console.log("Upload failed:", err);
		});
}

async function getHistoricalPriceCMC(id, rank) {
	try {
		// id,rank,open,close,timestamp
		const res = await instance.get(
			`/historical?id=${id}&convertId=2781&timeStart=${timeStart}&timeEnd=${timeEnd}`
		);
		const quotes = res.data.data.quotes;

		for (const { quote } of quotes) {
			const row = [
				id,
				rank,
				quote.open.toLocaleString("fullwide", {
					useGrouping: false,
					maximumSignificantDigits: 20,
				}),
				quote.close.toLocaleString("fullwide", {
					useGrouping: false,
					maximumSignificantDigits: 20,
				}),
				formatTimestamp(quote.timestamp),
			].toString();

			writeStream.write(row + "\r\n");
		}
	} catch (error) {
		console.log(error.message);
		console.log("wait 10 seconds to continue");
		await delay(10000);
		return getHistoricalPriceCMC(id, rank);
	}
}

async function main() {
	for (const { id, rank } of topCMC) {
		await getHistoricalPriceCMC(id, rank);
	}

	writeStream.end();

	await uploadToS3();
}

main().catch(console.log);
