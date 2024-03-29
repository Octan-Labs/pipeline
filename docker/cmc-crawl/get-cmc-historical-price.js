const axios = require("axios");
const fs = require("fs");
const {
	formatTimestamp,
	delay,
	uploadToS3,
	readFromS3,
	topCmcFilename,
} = require("./utils");
require("dotenv").config();

const startDate = process.env.START_DATE;
const endDate = process.env.END_DATE;
const HISTORICAL_BUCKET = process.env.AWS_S3_BUCKET_HISTORICAL;
const TOP_CMC_BUCKET = process.env.AWS_S3_BUCKET_TOP_CMC;
const REGION = process.env.S3_REGION;
const ACCESS_KEY = process.env.AWS_ACCESS_KEY_ID;
const SECRET_KEY = process.env.AWS_SECRET_ACCESS_KEY;

if (!startDate || !endDate) {
	console.log("TIME ENV is required");
	process.exit(1);
}

if (
	!HISTORICAL_BUCKET ||
	!TOP_CMC_BUCKET ||
	!REGION ||
	!ACCESS_KEY ||
	!SECRET_KEY
) {
	console.log("AWS ENV is required");
	process.exit(1);
}

const timeStart = Math.floor(new Date(startDate).setUTCHours(0, 0, 0) / 1000);
const timeEnd = Math.floor(new Date(endDate).setUTCHours(23, 59, 59) / 1000);

const filename = `start=${startDate}_end=${endDate}.csv`;

const instance = axios.create({
	baseURL: "https://api.coinmarketcap.com/data-api/v3/cryptocurrency",
});

const writeStream = fs.WriteStream(`./${filename}`);

async function getHistoricalPriceCMC(id, rank) {
	try {
		/**
		 * @param id CMC_ID of token get from s3
		 * @param timeStart start time to get historical price
		 * @param timeEnd end time to get historical price
		 * @returns historical price of token
		 */
		const res = await instance.get(
			`/historical?id=${id}&convertId=2781&timeStart=${timeStart}&timeEnd=${timeEnd}`
		);
		const quotes = res.data.data.quotes;

		for (const { quote } of quotes) {
			// id,rank,open,close,timestamp
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
		console.log("wait 10 seconds to continue");
		await delay(10000);
		return getHistoricalPriceCMC(id, rank);
	}
}

async function main() {
	try {
		const data = await readFromS3(
			ACCESS_KEY,
			SECRET_KEY,
			REGION,
			TOP_CMC_BUCKET,
			topCmcFilename
		);
		const topCMC = JSON.parse(data.Body.toString());
		for (const { id, rank } of topCMC) {
			await getHistoricalPriceCMC(id, rank);
		}
		writeStream.end();
		await uploadToS3(
			ACCESS_KEY,
			SECRET_KEY,
			REGION,
			HISTORICAL_BUCKET,
			filename
		);
	} catch (error) {
		console.log(error);
	}
}

main().catch(console.log);
