const axios = require("axios");
const fs = require("fs");
const { delay, uploadToS3, topCmcFilename } = require("./utils");
require("dotenv").config();

const CMC_API_KEY = process.env.CMC_API_KEY;
const BUCKET = process.env.AWS_S3_BUCKET_TOP_CMC;
const REGION = process.env.S3_REGION;
const ACCESS_KEY = process.env.AWS_ACCESS_KEY_ID;
const SECRET_KEY = process.env.AWS_SECRET_ACCESS_KEY;

if (!CMC_API_KEY) {
	console.log("CMC API KEY ENV is required");
	process.exit(1);
}

if (!BUCKET || !REGION || !ACCESS_KEY || !SECRET_KEY) {
	console.log("AWS ENV is required");
	process.exit(1);
}

const limit = process.env.LIMIT || 5000;

async function main() {
	try {
		/**
		 * Get top token from CMC sort by marketcap rank
		 * @param limit size limit 's number of top token, default 5000
		 * @param CMC_API_KEY CMC API key (get from https://pro.coinmarketcap.com/login/)
		 * @returns List top CMC
		 * example
		 * [
				{
					"id": 1, # CMC_ID
					"rank": 1,
					"name": "Bitcoin",
					"symbol": "BTC",
					"slug": "bitcoin",
					"is_active": 1,
					"platform": null
				}
			]
		 * !!! CMC_ID is REQUIRED to get historical price of its token
		 */
		const res = await axios.get(
			`https://pro-api.coinmarketcap.com/v1/cryptocurrency/map?sort=cmc_rank&limit=${limit}`,
			{
				headers: {
					"X-CMC_PRO_API_KEY": CMC_API_KEY,
				},
			}
		);
		data = res.data.data;

		fs.writeFileSync(`./${topCmcFilename}`, JSON.stringify(res.data.data));

		// sync top CMC to s3
		await uploadToS3(ACCESS_KEY, SECRET_KEY, REGION, BUCKET, topCmcFilename);
	} catch (error) {
		console.log("wait 10 seconds to continue");
		await delay(10000);
		return main();
	}
}

main();
