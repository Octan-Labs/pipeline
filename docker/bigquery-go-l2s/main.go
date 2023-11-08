package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/go-pg/pg/v10"
	"github.com/joho/godotenv"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type GasSpentRow struct {
	Date     string `bigquery:"date"`
	GasSpent int64  `bigquery:"gas_spent"`
}

type L2GasSpentTimeseriesValue struct {
	Date      time.Time
	ProjectId string
	GasSpent  int64
}

const OP_LABEL_ID = "recuGSDmg9OO6"

func queryDailyGasSpent(ctx context.Context, client *bigquery.Client, beginDate string, endDate string) (*bigquery.RowIterator, error) {
	query := client.Query(
		`SELECT CAST(DATE(blocks.block_timestamp) as STRING) AS date, SUM(gas_used) AS gas_spent
                FROM ` + "`bigquery-public-data.goog_blockchain_optimism_mainnet_us.blocks`" + ` AS blocks
				WHERE gas_used > 0
				AND DATE(blocks.block_timestamp) BETWEEN @begin_date AND @end_date
				GROUP BY date
                ORDER BY date ASC
                LIMIT 100;`)
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
		var row GasSpentRow
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

		l2GasSpent := &L2GasSpentTimeseriesValue{
			Date:      date,
			ProjectId: OP_LABEL_ID,
			GasSpent:  row.GasSpent,
		}
		_, err = db.Model(l2GasSpent).Insert()
		if err != nil {
			return fmt.Errorf("error when inserting record to Postgres: %w", err)
		}
		insertedRecord += 1
	}

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

	rows, err := queryDailyGasSpent(ctx, client, beginDate, endDate)
	if err != nil {
		log.Fatal(err)
	}

	if err := saveResult(pgUrl, os.Stdout, rows); err != nil {
		log.Fatal(err)
	}
}
