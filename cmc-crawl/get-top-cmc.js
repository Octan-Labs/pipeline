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

		await uploadToS3(ACCESS_KEY, SECRET_KEY, REGION, BUCKET, topCmcFilename);
	} catch (error) {
		console.log("wait 10 seconds to continue");
		await delay(10000);
		return main();
	}
}

main();
