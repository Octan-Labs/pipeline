const axios = require("axios");
const fs = require("fs");
require("dotenv").config();

const CMC_API_KEY = process.env.CMC_API_KEY;

if (!CMC_API_KEY) {
	console.log("CMC API KEY ENV is required");
	process.exit(1);
}

const limit = 5000;

const filename = "cmc_top.json";

async function main() {
	const res = await axios.get(
		`https://pro-api.coinmarketcap.com/v1/cryptocurrency/map?sort=cmc_rank&limit=${limit}`,
		{
			headers: {
				"X-CMC_PRO_API_KEY": CMC_API_KEY,
			},
		}
	);
	data = res.data.data;

	fs.writeFileSync(`./${filename}`, JSON.stringify(res.data.data));
}

main().catch(console.log);
