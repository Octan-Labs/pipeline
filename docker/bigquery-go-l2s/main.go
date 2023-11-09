package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/go-pg/pg/v10"
	"github.com/joho/godotenv"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type TransactionDataRow struct {
	Date    string `bigquery:"date"`
	Txn     int64  `bigquery:"txn"`
	Uaw     int64  `bigquery:"uaw"`
	GasUsed int64  `bigquery:"gas_used"`
	Volume  string `bigquery:"volume"`
}

type L2TransactionTimeseriesValue struct {
	Date      time.Time
	ProjectId string
	Txn       int64
	GasUsed   int64
	Uaw       int64
	Volume    float64
}

const OP_LABEL_ID = "recuGSDmg9OO6"

func queryTransactionData(ctx context.Context, client *bigquery.Client, beginDate string, endDate string) (*bigquery.RowIterator, error) {
	query := client.Query(
		`SELECT CAST(DATE(blocks.block_timestamp) as STRING) AS date,
		SUM(blocks.gas_used) AS gas_used,
		COUNT(transactions.block_hash) as txn,
		COUNT(DISTINCT transactions.from_address) as uaw,
		CAST(SUM(transactions.value.bignumeric_value) AS STRING) as volume
            FROM (
				SELECT *
				FROM ` + "`bigquery-public-data.goog_blockchain_optimism_mainnet_us.blocks`" + `
				WHERE gas_used > 0
				AND DATE(block_timestamp) BETWEEN @begin_date AND @end_date
			) AS blocks 
			JOIN ` + "`bigquery-public-data.goog_blockchain_optimism_mainnet_us.transactions`" + `AS transactions
			ON blocks.block_hash = transactions.block_hash
			GROUP BY date
            ORDER BY date ASC
            LIMIT 1000;`)
	query.Parameters = []bigquery.QueryParameter{
		{
			Name:  "begin_date",
			Value: beginDate,
		},
		{
			Name:  "end_date",
			Value: endDate,
		},
	}
	return query.Read(ctx)
}

func saveResult(connString string, w io.Writer, iter *bigquery.RowIterator) error {
	opt, err := pg.ParseURL(connString)
	if err != nil {
		panic(err)
	}

	db := pg.Connect(opt)
	defer db.Close()
	insertedRecord := 0

	for {
		var row TransactionDataRow
		err := iter.Next(&row)
		if err == iterator.Done {
			fmt.Fprintf(w, "inserted %d daily gas spent records to Postgres\n", insertedRecord)
			return nil
		}
		if err != nil {
			return fmt.Errorf("error iterating through results: %w", err)
		}

		date, err := time.Parse("2006-01-02", row.Date)
		if err != nil {
			return fmt.Errorf("error parsing time string: %w", err)
		}

		l2GasSpent := &L2TransactionTimeseriesValue{
			Date:      date,
			ProjectId: OP_LABEL_ID,
			GasUsed:   row.GasUsed,
			Txn:       row.Txn,
			Uaw:       row.Uaw,
			Volume:    weiToEth(row.Volume),
		}
		_, err = db.Model(l2GasSpent).Insert()
		if err != nil {
			return fmt.Errorf("error when inserting record to Postgres: %w", err)
		}
		insertedRecord += 1
	}
}

// Convert the Wei value as a big.Rat to ETH as a float64
func weiToEth(weiValue string) float64 {
	// Convert the Wei value to ETH as a float64
	weiBigInt, success := new(big.Int).SetString(weiValue, 10)
	if !success {
		log.Fatal("Failed to parse Wei value")
	}

	// Define the ETH to Wei conversion ratio (1 ETH = 10^18 Wei)
	ethToWeiRatio := new(big.Float).SetInt64(1e18)

	// Calculate ETH value as a float64
	ethFloat := new(big.Float).Quo(new(big.Float).SetInt(weiBigInt), ethToWeiRatio)
	ethFloat64, _ := ethFloat.Float64()
	return ethFloat64
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("err loading env variables: %v", err)
	}

	requiredEnvs := map[string]string{
		"GOOGLE_CLOUD_PROJECT":           "",
		"GOOGLE_APPLICATION_CREDENTIALS": "",
		"BEGIN_DATE":                     "",
		"END_DATE":                       "",
		"POSTGRES_URL":                   "",
	}

	for env := range requiredEnvs {
		value := os.Getenv(env)
		if value == "" {
			fmt.Printf("%s environment variable must be set.\n", env)
			os.Exit(1)
		}
		requiredEnvs[env] = value
	}

	projectID := requiredEnvs["GOOGLE_CLOUD_PROJECT"]
	credentialFile := requiredEnvs["GOOGLE_APPLICATION_CREDENTIALS"]
	beginDate := requiredEnvs["BEGIN_DATE"]
	endDate := requiredEnvs["END_DATE"]
	pgUrl := requiredEnvs["POSTGRES_URL"]

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(credentialFile))
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	rows, err := queryTransactionData(ctx, client, beginDate, endDate)
	if err != nil {
		log.Fatal(err)
	}

	if err := saveResult(pgUrl, os.Stdout, rows); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}
