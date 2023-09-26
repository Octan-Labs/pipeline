INSERT INTO FUNCTION
   s3(
       'https://bucket.s3.ap-southeast-1.amazonaws.com/path/file.csv',
	   'access_key', 
	   'secret_key', 
       'CSV'
    )
SELECT 
  hash AS tx_hash, 
  block_number, 
  from_address, 
  to_address, 
  gas, 
  gas_used, 
  gas_price, 
  value, 
  null AS token_transfer, 
  null AS token_address, 
  (value / POWER(10, 18)) * cmc_h.avg_price AS volume, 
  (gas_used * gas_price) / POWER(10, 18) AS gas_spent, 
  (gas_used * gas_price) / POWER(10, 18) * cmc_h.avg_price AS gas_spent_usd 
FROM (
	SELECT 
    hash, 
    block_number, 
    from_address, 
    to_address, 
    gas, 
    receipt_gas_used AS gas_used, 
    gas_price, 
    value, 
    toDate(block_timestamp) AS tx_timestamp 
  FROM 
    bsc_transaction FINAL -- bsc_transaction | eth_transaction
  WHERE toDate(block_timestamp) >= '2023-08-01' AND toDate(block_timestamp) <= '2023-08-01'
) AS tx
LEFT JOIN (
	SELECT id, (open + close) / 2 as avg_price, toDate(timestamp) as cmc_timestamp 
	FROM cmc_historical FINAL 
	WHERE toDate(timestamp) >= '2023-08-01' AND toDate(timestamp) <= '2023-08-01'
) AS cmc_h ON tx_timestamp = cmc_timestamp AND cmc_h.id = 1839 -- BNB=1839 | ETH=1027
UNION ALL 
SELECT 
  tt.transaction_hash AS tx_hash, 
  tt.block_number AS block_number, 
  tt.from_address, 
  to_address, 
  null AS gas, 
  null AS gas_used, 
  null AS gas_price, 
  null AS value, 
  token_transfer, 
  token_address, 
  token_transfer / POWER(10, t.decimals) * cmc_h.avg_price AS volume, 
  null AS gas_spent, 
  null AS gas_spent_usd
FROM (
	SELECT 
		block_number,
		CONCAT('0x', RIGHT(arrayElement(topics, 2), 40)) AS from_address, 
		CONCAT('0x', RIGHT(arrayElement(topics, 3), 40)) AS to_address, 
		reinterpretAsUInt256(reverse(unhex(data))) AS token_transfer, 
		address as token_address, 
		transaction_hash, 
		toDate(block_timestamp) as tx_timestamp 
	FROM bsc_log --  bsc_log | eth_log
	WHERE toDate(block_timestamp) >= '2023-08-01' AND toDate(block_timestamp) <= '2023-08-01' 
	AND LENGTH(topics) = 3 AND arrayElement(topics, 1) = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
  ) AS tt 
LEFT JOIN cmc_bsc_token t -- cmc_bsc_token | cmc_eth_token
	ON LOWER(tt.token_address) = LOWER(t.address) 
LEFT JOIN cmc_address cmc_addr ON tt.token_address = cmc_addr.bsc -- cmc_addr.bsc | cmc_addr.eth
LEFT JOIN (
	SELECT id, (open + close) / 2 as avg_price, toDate(timestamp) as cmc_timestamp 
	FROM cmc_historical FINAL 
	WHERE toDate(timestamp) >= '2023-08-01' AND toDate(timestamp) <= '2023-08-01'
  ) AS cmc_h ON cmc_addr.id = cmc_h.id AND tx_timestamp = cmc_h.cmc_timestamp
  
  
  