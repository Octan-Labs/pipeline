const fs = require("fs");
const AWS = require("aws-sdk");

function formatTimestamp(timestamp) {
	return Math.floor(new Date(timestamp).getTime() / 1000);
}

function delay(t, v) {
	return new Promise((resolve) => setTimeout(resolve, t, v));
}

async function uploadToS3(accessKeyId, secretAccessKey, region, bucket, key) {
	AWS.config.update({
		accessKeyId,
		secretAccessKey,
		region,
	});
	var s3 = new AWS.S3();

	console.log("Upload file to S3");

	s3.putObject({
		Bucket: bucket,
		Body: fs.readFileSync(`./${key}`),
		Key: key,
	})
		.promise()
		.then((res) => {
			console.log(`Upload succeeded - `, res);
		})
		.catch((err) => {
			console.log("Upload failed:", err);
		});
}

function readFromS3(accessKeyId, secretAccessKey, region, bucket, key) {
	AWS.config.update({
		accessKeyId,
		secretAccessKey,
		region,
	});
	var s3 = new AWS.S3();

	return s3.getObject({ Bucket: bucket, Key: key }).promise();
}

const topCmcFilename = "cmc_top.json";

module.exports = {
	formatTimestamp,
	delay,
	uploadToS3,
	readFromS3,
	topCmcFilename,
};
