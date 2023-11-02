from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
}

with DAG(
        dag_id='eth_export_dashboard_query_output_to_postgresql',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=['eth', 'dashboard']
) as dag:
    connection = BaseHook.get_connection("pg_connection")
    conn_password = connection.password
    conn_login = connection.login
    conn_host = connection.host
    conn_port = connection.port
    conn_schema = connection.schema

    eth_global_reputation_score_overview_var = PostgresOperator(
        task_id="truncate_eth_global_reputation_score_overview_var",
        sql="""
            truncate eth_global_reputation_score_overview_var
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_global_reputation_score_overview_var',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                    '{host}:{port}', 
                    '{database}', 
                    'eth_global_reputation_score_overview_var', 
                    '{user}', 
                    '{password}'
                )
                (id, categories, reputation_score, total_transfer, total_receive, in_degree, out_degree, total_volume, total_gas_spent)
                select 
                    id,
                    categories,
                    reputation_score,
                    total_transfer,
                    total_receive,
                    in_degree,
                    out_degree,
                    total_volume,
                    total_gas_spent
                    from (SELECT
                        1 as id,
                        'All addresses' AS categories,
                        VAR_SAMP(reputation_score) AS reputation_score,
                        VAR_SAMP(total_transfer) AS total_transfer,
                        VAR_SAMP(total_receive) AS total_receive,
                        VAR_SAMP(in_degree) AS in_degree,
                        VAR_SAMP(out_degree) AS out_degree,
                        VAR_SAMP(total_volume) AS total_volume,
                        VAR_SAMP(total_gas_spent) AS total_gas_spent
                    FROM
                        eth_reputation_score rs
                    WHERE
                        rs.snapshot_block_number=(select max(snapshot_block_number) from eth_reputation_score)
                    UNION ALL
                    SELECT
                        2 as id,
                        'Contracts' AS categories,
                        VAR_SAMP(reputation_score) AS reputation_score,
                        VAR_SAMP(total_transfer) AS total_transfer,
                        VAR_SAMP(total_receive) AS total_receive,
                        VAR_SAMP(in_degree) AS in_degree,
                        VAR_SAMP(out_degree) AS out_degree,
                        VAR_SAMP(total_volume) AS total_volume,
                        VAR_SAMP(total_gas_spent) AS total_gas_spent
                    FROM
                        eth_reputation_score rs
                    WHERE
                        rs.contract = True
                    AND
                        rs.snapshot_block_number=(select max(snapshot_block_number) from eth_reputation_score)
                    UNION ALL
                    SELECT
                        3 as id,
                        'EOAs' AS categories,
                        VAR_SAMP(reputation_score) AS reputation_score,
                        VAR_SAMP(total_transfer) AS total_transfer,
                        VAR_SAMP(total_receive) AS total_receive,
                        VAR_SAMP(in_degree) AS in_degree,
                        VAR_SAMP(out_degree) AS out_degree,
                        VAR_SAMP(total_volume) AS total_volume,
                        VAR_SAMP(total_gas_spent) AS total_gas_spent
                    FROM
                        eth_reputation_score rs
                    WHERE
                        rs.contract = False
                    AND
                        rs.snapshot_block_number=(select max(snapshot_block_number) from eth_reputation_score)
                    UNION ALL
                    SELECT
                        4 as id,
                        'EOA*s' AS categories,
                        VAR_SAMP(reputation_score) AS reputation_score,
                        VAR_SAMP(total_transfer) AS total_transfer,
                        VAR_SAMP(total_receive) AS total_receive,
                        VAR_SAMP(in_degree) AS in_degree,
                        VAR_SAMP(out_degree) AS out_degree,
                        VAR_SAMP(total_volume) AS total_volume,
                        VAR_SAMP(total_gas_spent) AS total_gas_spent
                    FROM
                        eth_reputation_score rs
                    WHERE
                        rs.contract = False
                        AND
                        rs.reputation_score_log_scale < 500
                        AND
                        rs.reputation_score_log_scale > 0
                    AND
                        rs.snapshot_block_number=(select max(snapshot_block_number) from eth_reputation_score)
                    ORDER BY
                    total_transfer desc
                ) eth_global_reputation_score_overview_var;
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_global_reputation_score_overview_mean = PostgresOperator(
        task_id="truncate_eth_global_reputation_score_overview_mean",
        sql="""
            truncate eth_global_reputation_score_overview_mean
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_global_reputation_score_overview_mean',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                    '{host}:{port}', 
                    '{database}', 
                    'eth_global_reputation_score_overview_mean', 
                    '{user}', 
                    '{password}'
                )
                (id, categories, reputation_score, total_transfer, total_receive, in_degree, out_degree, total_volume, total_gas_spent)
                select 
                    id,
                    categories,
                    reputation_score,
                    total_transfer,
                    total_receive,
                    in_degree,
                    out_degree,
                    total_volume,
                    total_gas_spent
                    from (SELECT
                            1 as id,
                            'All addresses' AS categories,
                            AVG(reputation_score) AS reputation_score,
                            AVG(total_transfer) AS total_transfer,
                            AVG(total_receive) AS total_receive,
                            AVG(in_degree) AS in_degree,
                            AVG(out_degree) AS out_degree,
                            AVG(total_volume) AS total_volume,
                            AVG(total_gas_spent) AS total_gas_spent
                        FROM
                            eth_reputation_score rs
                        WHERE
                            rs.snapshot_block_number=(select max(snapshot_block_number) from eth_reputation_score)
                        UNION ALL
                        SELECT
                            2 as id,
                            'Contracts' AS categories,
                            AVG(reputation_score) AS reputation_score,
                            AVG(total_transfer) AS total_transfer,
                            AVG(total_receive) AS total_receive,
                            AVG(in_degree) AS in_degree,
                            AVG(out_degree) AS out_degree,
                            AVG(total_volume) AS total_volume,
                            AVG(total_gas_spent) AS total_gas_spent
                        FROM
                            eth_reputation_score rs
                        WHERE
                            rs.contract = True
                        AND
                            rs.snapshot_block_number=(select max(snapshot_block_number) from eth_reputation_score)
                        UNION ALL
                        SELECT
                            3 as id,
                            'EOAs' AS categories,
                            AVG(reputation_score) AS reputation_score,
                            AVG(total_transfer) AS total_transfer,
                            AVG(total_receive) AS total_receive,
                            AVG(in_degree) AS in_degree,
                            AVG(out_degree) AS out_degree,
                            AVG(total_volume) AS total_volume,
                            AVG(total_gas_spent) AS total_gas_spent
                        FROM
                            eth_reputation_score rs
                        WHERE
                            rs.contract = False
                        AND
                            rs.snapshot_block_number=(select max(snapshot_block_number) from eth_reputation_score)
                        UNION ALL
                        SELECT
                            4 as id,
                            'EOA*s' AS categories,
                            AVG(reputation_score) AS reputation_score,
                            AVG(total_transfer) AS total_transfer,
                            AVG(total_receive) AS total_receive,
                            AVG(in_degree) AS in_degree,
                            AVG(out_degree) AS out_degree,
                            AVG(total_volume) AS total_volume,
                            AVG(total_gas_spent) AS total_gas_spent
                        FROM
                            eth_reputation_score rs
                        WHERE
                            rs.contract = False
                            AND
                            rs.reputation_score_log_scale < 500
                            AND
                            rs.reputation_score_log_scale > 0
                        AND
                            rs.snapshot_block_number=(select max(snapshot_block_number) from eth_reputation_score)
                        ORDER BY
                        total_transfer desc
                        ) eth_global_reputation_score_overview_mean;
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_key_metric = PostgresOperator(
        task_id="truncate_eth_key_metric",
        sql="""
            truncate eth_key_metric
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_key_metric',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_key_metric', 
                '{user}', 
                '{password}'
            )
            (categories, value)
            SELECT categories, value FROM (
            WITH eth_reputation_score_tmp AS (
                SELECT *
                FROM eth_reputation_score
                WHERE snapshot_block_number IN (
                    SELECT MAX(snapshot_block_number)
                    FROM eth_reputation_score
                )
            )
            SELECT 
            'Total blocks' categories,
            CAST(((MAX(snapshot_block_number) - 15993179) + 1) AS DOUBLE) value 
            FROM eth_reputation_score_tmp 
            UNION ALL
            SELECT 
            'Total transactions' categories,
            CAST((SUM(total_transaction))AS DOUBLE) value 
            FROM (
                SELECT count() total_transaction
                FROM eth_transaction -- eth_transaction | bsc_transaction
                WHERE block_number >= 15993179 AND block_number <= (SELECT MAX(snapshot_block_number) FROM eth_reputation_score_tmp)
                UNION ALL 
                SELECT count() total_transaction
                FROM eth_log --  eth_log | bsc_log 
                WHERE LENGTH(topics) = 3 AND arrayElement(topics, 1) = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' AND
                    block_number >= 15993179 AND block_number <= (SELECT MAX(snapshot_block_number) FROM eth_reputation_score_tmp)
            )   
            UNION ALL
            SELECT 
            'Total gas spent (ETH)' categories,
            CAST((SUM(total_gas_spent))AS DOUBLE) AS total_gas_spent
            FROM eth_reputation_score_tmp
            UNION ALL
            SELECT 
            'Total address' categories,
            CAST((COUNT())AS DOUBLE)  value 
            FROM eth_reputation_score_tmp 
            UNION ALL
            SELECT 
            'Total contracts' categories,
            CAST((COUNT())AS DOUBLE)  value 
            FROM eth_reputation_score_tmp 
            WHERE contract = TRUE
            UNION ALL 
            SELECT 
            'Total contracts with zero-GRS' categories,
            CAST((COUNT())AS DOUBLE)  value 
            FROM eth_reputation_score_tmp 
            WHERE contract = TRUE AND reputation_score = 0
            UNION ALL 
            SELECT 
            'Total EOAs' categories,
            CAST((COUNT())AS DOUBLE)  value 
            FROM eth_reputation_score_tmp 
            WHERE contract = FALSE
            UNION ALL 
            SELECT 
            'Total EOAs with zero-GRS' categories,
            CAST((COUNT())AS DOUBLE)  value 
            FROM eth_reputation_score_tmp 
            WHERE contract = FALSE AND reputation_score = 0
            UNION ALL 
            SELECT 
            'Average GRS (Contract Group)' categories,
            CAST((SUM(reputation_score) / COUNT())AS DOUBLE)  value 
            FROM eth_reputation_score_tmp 
            WHERE contract = TRUE
            UNION ALL 
            SELECT 
            'Average GRS (EOA Group)' categories,
            CAST((SUM(reputation_score) / COUNT())AS DOUBLE)  value 
            FROM eth_reputation_score_tmp 
            WHERE contract = FALSE
            UNION ALL 
            SELECT 
            'Average GRS (Non-zero EOA Group)' categories,
            CAST((SUM(reputation_score) / COUNT())AS DOUBLE) value 
            FROM eth_reputation_score_tmp 
            WHERE contract = FALSE AND reputation_score != 0
            UNION ALL
            SELECT 
            'Variance (Contract Group)' categories,
            CAST((VAR_SAMP(reputation_score))AS DOUBLE)  value 
            FROM eth_reputation_score_tmp 
            WHERE contract = TRUE
            UNION ALL
            SELECT 
            'Variance (EOA Group)' categories,
            CAST((VAR_SAMP(reputation_score))AS DOUBLE)  value 
            FROM eth_reputation_score_tmp 
            WHERE contract = FALSE) eth_key_metric
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_rs_all = PostgresOperator(
        task_id="truncate_eth_rs_all",
        sql="""
            truncate eth_rs_all
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_rs_all',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_rs_all', 
                '{user}', 
                '{password}'
            )
            (bin_floor, log_count)
            SELECT bin_floor, log_count FROM (
                WITH rs_table AS (
                    SELECT *
                    FROM eth_reputation_score ers
                    WHERE ers.snapshot_block_number = (select max(snapshot_block_number) from eth_reputation_score)
                )
                SELECT CAST( FLOOR(reputation_score/ 5) * 5 as INT) as bin_floor,
                        LOG10(COUNT(*)) as log_count
                FROM rs_table
                GROUP BY bin_floor
            ) eth_rs_all
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_rs_all_log = PostgresOperator(
        task_id="truncate_eth_rs_all_log",
        sql="""
            truncate eth_rs_all_log
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_rs_all_log',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_rs_all_log', 
                '{user}', 
                '{password}'
            )
            (bin_floor, log_count)
            SELECT bin_floor, log_count FROM (
                WITH rs_table AS (
                    SELECT *
                    FROM eth_reputation_score ers
                    WHERE ers.snapshot_block_number = (select max(snapshot_block_number) from eth_reputation_score)
                )
                SELECT CAST( FLOOR(reputation_score_log_scale / 5) * 5 as INT) as bin_floor, LOG10(COUNT(*)) as log_count
                FROM rs_table
                GROUP BY bin_floor
            ) eth_rs_all_log
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_rs_contract_log = PostgresOperator(
        task_id="truncate_eth_rs_contract_log",
        sql="""
            truncate eth_rs_contract_log
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_rs_contract_log',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_rs_contract_log', 
                '{user}', 
                '{password}'
            )
            (bin_floor, log_count)
            SELECT bin_floor, log_count FROM (
                WITH rs_table AS (
                    SELECT *
                    FROM eth_reputation_score ers
                    WHERE ers.snapshot_block_number = (select max(snapshot_block_number) from eth_reputation_score)
                    AND ers.contract=True
                )
                SELECT CAST( FLOOR(reputation_score_log_scale / 5) * 5 as INT) as bin_floor, LOG10(COUNT(*)) as log_count
                FROM rs_table
                GROUP BY bin_floor
            ) eth_rs_contract_log
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_rs_eoa = PostgresOperator(
        task_id="truncate_eth_rs_eoa",
        sql="""
            truncate eth_rs_eoa
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_rs_eoa',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_rs_eoa', 
                '{user}', 
                '{password}'
            )
            (bin_floor, log_count)
            SELECT bin_floor, log_count FROM (
                WITH rs_table AS (
                    SELECT *
                    FROM eth_reputation_score ers
                    WHERE ers.snapshot_block_number = (select max(snapshot_block_number) from eth_reputation_score)
                    AND ers.contract=False
                )
                SELECT CAST( FLOOR(reputation_score/ 5) * 5 as INT) as bin_floor, LOG10(COUNT(*)) as log_count
                FROM rs_table
                GROUP BY bin_floor
            ) eth_rs_eoa
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_rs_eoa_log = PostgresOperator(
        task_id="truncate_eth_rs_eoa_log",
        sql="""
            truncate eth_rs_eoa_log
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_rs_eoa_log',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_rs_eoa_log', 
                '{user}', 
                '{password}'
            )
            (bin_floor, log_count)
            SELECT bin_floor, log_count FROM (
                WITH rs_table AS (
                    SELECT *
                    FROM eth_reputation_score ers
                    WHERE ers.snapshot_block_number = (select max(snapshot_block_number) from eth_reputation_score)
                    AND ers.contract=False
                )
                SELECT CAST( FLOOR(reputation_score_log_scale / 5) * 5 as INT) as bin_floor, LOG10(COUNT(*)) as log_count
                FROM rs_table
                GROUP BY bin_floor
            ) eth_rs_eoa_log
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_main_groups = PostgresOperator(
        task_id="truncate_eth_main_groups",
        sql="""
            truncate eth_main_groups
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_main_groups',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_main_groups', 
                '{user}', 
                '{password}'
            )
            (groups, num_of_addresses, num_of_addresses_percentage, total_reputation_score, total_reputation_score_percentage)
            SELECT groups, num_of_addresses, num_of_addresses_percentage, total_reputation_score, total_reputation_score_percentage FROM (
                WITH eth_reputation_score_tmp AS (
                    SELECT *
                    FROM eth_reputation_score
                    WHERE snapshot_block_number IN (
                            SELECT MAX(snapshot_block_number)
                            FROM eth_reputation_score
                        )
                    )
                    SELECT 
                        'Non-zero EOAs' groups,
                        COUNT() num_of_addresses,
                        ROUND(num_of_addresses * 100 / (SELECT COUNT() FROM eth_reputation_score_tmp), 2) num_of_addresses_percentage,
                        SUM(reputation_score) total_reputation_score,
                        ROUND(divideDecimal(total_reputation_score * 100, (SELECT SUM(reputation_score) FROM eth_reputation_score_tmp), 10), 2) total_reputation_score_percentage
                    FROM eth_reputation_score_tmp 
                    WHERE contract = FALSE AND reputation_score != 0
                    UNION ALL 
                    SELECT 
                        'Zero EOAs' groups,
                        COUNT() num_of_addresses,
                        ROUND(num_of_addresses * 100 / (SELECT COUNT() FROM eth_reputation_score_tmp), 2) num_of_addresses_percentage,
                        0 total_reputation_score,
                        0 total_reputation_score_percentage
                    FROM eth_reputation_score_tmp 
                    WHERE contract = FALSE AND reputation_score = 0
                    UNION ALL 
                    SELECT 
                        'Contracts' groups,
                        COUNT() num_of_addresses,
                        ROUND(num_of_addresses * 100 / (SELECT COUNT() FROM eth_reputation_score_tmp), 2) num_of_addresses_percentage,
                        SUM(reputation_score) total_reputation_score,
                        ROUND(divideDecimal(total_reputation_score * 100, (SELECT SUM(reputation_score) FROM eth_reputation_score_tmp), 10), 2) total_reputation_score_percentage
                    FROM eth_reputation_score_tmp 
                    WHERE contract = TRUE
            ) eth_main_groups
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_rs_distribution_contract_categories = PostgresOperator(
        task_id="truncate_eth_rs_distribution_contract_categories",
        sql="""
            truncate eth_rs_distribution_contract_categories
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_rs_distribution_contract_categories',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_rs_distribution_contract_categories', 
                '{user}', 
                '{password}'
            )
            (contract_categories, uaws, avg_rs, total_reputation_score, num_of_addresses, total_reputation_score_percentage, num_of_addresses_percentage)
            SELECT contract_categories, uaws, avg_rs, total_reputation_score, num_of_addresses, total_reputation_score_percentage, num_of_addresses_percentage FROM (
                WITH eth_reputation_score_tmp AS (
                SELECT *
                FROM eth_reputation_score
                WHERE snapshot_block_number = (
                    SELECT MAX(snapshot_block_number)
                    FROM eth_reputation_score)
                AND contract = TRUE
                ), combine_table_1 AS (
                SELECT tmp.*,
                    CASE 
                        WHEN la.category IS NULL OR la.category = '' THEN 'Unknown' -- in case wrong information from apitable
                        ELSE la.category
                    END AS category
                FROM eth_reputation_score_tmp tmp
                LEFT JOIN label_eth_contract la ON LOWER(tmp.address) = LOWER(la.address) -- also in case 
                ), combined_table_1_remove_duplicate AS (
                SELECT *
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY address) AS rn
                    FROM combine_table_1
                ) ranked
                WHERE rn = 1 -- remove duplicate of address
                ), counting_table_1 AS (
                    SELECT 
                        category,
                        CAST(SUM(reputation_score) AS DOUBLE) AS trep,
                        CAST(COUNT(DISTINCT address) AS INT) AS noa,
                        CAST(SUM(SUM(reputation_score)) OVER () AS DOUBLE) total_trep,
                        CAST(SUM(COUNT(DISTINCT address)) OVER () AS DOUBLE) total_noa
                    FROM combined_table_1_remove_duplicate
                    GROUP BY category
                    ORDER BY category
                ), result_1 AS( 
                SELECT
                    category,
                    trep,
                    noa,
                    trep*100/total_trep as trep_p,
                    noa*100/total_noa as noa_p
                FROM
                    counting_table_1
                ), combine_table_2 AS ( -- GET SECOND RESULT
                SELECT
                from_address,
                CASE 
                    WHEN lec.category IS NULL OR lec.category = '' THEN 'Unknown'
                    ELSE lec.category
                END AS category,
                max(reputation_score) as RS-- must put in a aggregate function but actually each from address onlyu have one reputation_score
                FROM
                eth_transaction AS et
                INNER JOIN (
                SELECT
                    address,
                    category
                FROM
                    label_eth_contract
                ) AS lec ON LOWER(et.to_address) = LOWER(lec.address)
                INNER JOIN (
                SELECT
                    address,
                    reputation_score
                FROM
                    eth_reputation_score
                WHERE
                    contract = FALSE
                    AND snapshot_block_number = (
                    SELECT MAX(snapshot_block_number) FROM eth_reputation_score
                    )
                    AND reputation_score < 651.3227399106263 -- highest limit
                ) AS ers ON et.from_address = ers.address
                WHERE
                toDate(et.block_timestamp) >= '2023-01-01'
                GROUP BY
                et.from_address, -- this is where error happen, one from address don't need to belong to one category only
                category
                ),result_2 as (
                SELECT --aggregate from middle table to get the final one
                    category,
                    count(distinct from_address) as UAW,
                    avg(RS) as avg_rs
                    FROM
                        combine_table_2
                    GROUP BY
                        category
                ) SELECT
                    r1.category as contract_categories,
                    UAW as uaws,
                    avg_rs,
                    trep as total_reputation_score,
                    noa as num_of_addresses,
                    trep_p as total_reputation_score_percentage,
                    noa_p as num_of_addresses_percentage
                FROM result_2 r2 -- COMBINE BOTH RESULT
                INNER JOIN result_1 r1 
                on r1.category = r2.category
            ) eth_rs_distribution_contract_categories
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_average_distribution_on_5_partitions_of_contract_group = PostgresOperator(
        task_id="truncate_eth_average_distribution_on_5_partitions_of_contract_group",
        sql="""
            truncate eth_average_distribution_on_5_partitions_of_contract_group
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_average_distribution_on_5_partitions_of_contract_group',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_average_distribution_on_5_partitions_of_contract_group', 
                '{user}', 
                '{password}'
            )
            (partition, reputation_score, total_transfer, total_receive, in_degree, out_degree, total_volume, total_gas_spent)
            SELECT partition, reputation_score, total_transfer, total_receive, in_degree, out_degree, total_volume, total_gas_spent FROM (
                WITH eth_reputation_score_tmp AS (
                    SELECT *
                    FROM eth_reputation_score
                    WHERE snapshot_block_number IN (
                        SELECT MAX(number)
                        FROM eth_block 
                        WHERE toDate(timestamp) >= '2022-11-18' AND toDate(timestamp) <= '2023-09-15'
                    ) AND contract = TRUE
                ), distribution_on_5_partitions_of_contract_group AS (
                SELECT 
                    CASE WHEN reputation_score_log_scale > 680 then '5. Excellent'
                    WHEN reputation_score_log_scale <= 680 AND reputation_score_log_scale > 300 then '4. High'
                    WHEN reputation_score_log_scale <= 300 AND reputation_score_log_scale > 100 then '3. Medium'
                    WHEN reputation_score_log_scale <= 100 AND reputation_score_log_scale > 0 then '2. Low'
                    ELSE '1. Zero-GRS' END partition,
                    ROUND(SUM(reputation_score) / COUNT(), 3) reputation_score,
                    ROUND(SUM(total_transfer) / COUNT(), 3) total_transfer,
                    ROUND(SUM(total_receive) / COUNT(), 3) total_receive,
                    ROUND(SUM(in_degree) / COUNT(), 3) in_degree,
                    ROUND(SUM(out_degree) / COUNT(), 3) out_degree,
                    ROUND(SUM(total_volume) / COUNT(), 3) total_volume,
                    ROUND(SUM(total_gas_spent) / COUNT(), 3) total_gas_spent
                FROM eth_reputation_score_tmp
                GROUP BY partition
                ORDER BY partition
                )
                SELECT * FROM distribution_on_5_partitions_of_contract_group
            ) eth_average_distribution_on_5_partitions_of_contract_group
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_variance_distribution_on_5_partitions_of_contract_group = PostgresOperator(
        task_id="truncate_eth_variance_distribution_on_5_partitions_of_contract_group",
        sql="""
            truncate eth_variance_distribution_on_5_partitions_of_contract_group
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_variance_distribution_on_5_partitions_of_contract_group',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_variance_distribution_on_5_partitions_of_contract_group', 
                '{user}', 
                '{password}'
            )
            (partition, reputation_score, total_transfer, total_receive, in_degree, out_degree, total_volume, total_gas_spent)
            SELECT partition, reputation_score, total_transfer, total_receive, in_degree, out_degree, total_volume, total_gas_spent FROM (
                WITH eth_reputation_score_tmp AS (
                    SELECT *
                    FROM eth_reputation_score
                    WHERE snapshot_block_number IN (
                        SELECT MAX(snapshot_block_number)
                        FROM eth_reputation_score 
                    ) AND contract = TRUE
                ), distribution_on_5_partitions_of_contract_group AS (
                SELECT 
                    CASE WHEN reputation_score_log_scale > 680 then '5. Excellent'
                    WHEN reputation_score_log_scale <= 680 AND reputation_score_log_scale > 300 then '4. High'
                    WHEN reputation_score_log_scale <= 300 AND reputation_score_log_scale > 100 then '3. Medium'
                    WHEN reputation_score_log_scale <= 100 AND reputation_score_log_scale > 0 then '2. Low'
                    ELSE '1. Zero-GRS' END partition,
                    VAR_SAMP(reputation_score) reputation_score,
                    VAR_SAMP(total_transfer) total_transfer,
                    VAR_SAMP(total_receive) total_receive,
                    VAR_SAMP(in_degree) in_degree,
                    VAR_SAMP(out_degree) out_degree,
                    VAR_SAMP(total_volume) total_volume,
                    VAR_SAMP(total_gas_spent) total_gas_spent
                FROM eth_reputation_score_tmp
                GROUP BY partition
                ORDER BY partition
                )
                SELECT * FROM distribution_on_5_partitions_of_contract_group
            ) eth_variance_distribution_on_5_partitions_of_contract_group
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_top10_contract_uaw = PostgresOperator(
        task_id="truncate_eth_top10_contract_uaw",
        sql="""
            truncate eth_top10_contract_uaw
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_top10_contract_uaw',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_top10_contract_uaw', 
                '{user}', 
                '{password}'
            )
            (rank, address, identity, reputation_score, in_degree, out_degree, total_transfer, total_receive, total_volume, total_gas_spent, uaw, average_rs)
            SELECT rank, address, identity, reputation_score, in_degree, out_degree, total_transfer, total_receive, total_volume, total_gas_spent, uaw, average_rs FROM (
                WITH top10contract AS (
                SELECT
                    ers.address AS contract_address,
                    ers.rank AS grank, --rank is a function name
                    ers.*,
                    lc.identity,
                FROM
                    eth_reputation_score ers
                INNER JOIN label_eth_contract lc ON lc.address = ers.address -- add identity
                WHERE
                    ers.snapshot_block_number = (
                        SELECT MAX(snapshot_block_number) FROM eth_reputation_score
                    )
                    AND
                    ers.contract = TRUE
                LIMIT
                    10
                ), combined_table AS (
                    SELECT DISTINCT
                        et.to_address AS contract_address,
                        et.from_address AS interact_address,
                        rs_table.RS AS RS
                    FROM
                        eth_transaction et
                    INNER JOIN (
                        SELECT
                            address,
                            reputation_score AS RS,
                            contract
                        FROM
                            eth_reputation_score
                        WHERE
                            snapshot_block_number = (SELECT MAX(snapshot_block_number) FROM eth_reputation_score)
                            AND
                            contract = FALSE 
                            AND
                            RS <= 500
                    ) rs_table
                    ON et.from_address = rs_table.address
                    WHERE
                        et.to_address IN (SELECT contract_address FROM top10contract)
                        AND et.block_number BETWEEN 15993179 AND (SELECT MAX(snapshot_block_number) FROM eth_reputation_score)
                ), top10_uaw AS (
                    SELECT
                        tc.contract_address AS contract_address,
                        COUNT(ct.interact_address) AS UAW,
                        AVG(ct.RS) AS avg_RS
                    FROM combined_table ct
                    INNER JOIN top10contract tc ON tc.contract_address = ct.contract_address
                    GROUP BY
                        tc.contract_address
                )
                SELECT
                    grank AS rank,
                    tc.contract_address AS address,
                    tc.identity as identity,
                    tc.reputation_score AS reputation_score,
                    tc.in_degree AS in_degree,
                    tc.out_degree AS out_degree,
                    tc.total_transfer AS total_transfer,
                    tc.total_receive AS total_receive,
                    tc.total_volume AS total_volume,
                    tc.total_gas_spent AS total_gas_spent,
                    UAW as uaw,
                    avg_RS AS average_rs
                FROM
                    top10_uaw tu
                INNER JOIN
                    top10contract tc ON tu.contract_address = tc.contract_address
            ) eth_top10_contract_uaw
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_nzeoa_overview = PostgresOperator(
        task_id="truncate_eth_nzeoa_overview",
        sql="""
            truncate eth_nzeoa_overview
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_nzeoa_overview',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_nzeoa_overview', 
                '{user}', 
                '{password}'
            )
            (categories, reputation_score, total_transfer, total_receive, in_degree, out_degree, total_volume, total_gas_spent )
            SELECT categories, reputation_score, total_transfer, total_receive, in_degree, out_degree, total_volume, total_gas_spent  FROM (
                WITH rs_table AS (
                    SELECT * FROM eth_reputation_score rs
                    WHERE rs.snapshot_block_number = (SELECT MAX(snapshot_block_number) FROM eth_reputation_score)
                    AND rs.contract = False
                    AND rs.reputation_score > 0
                )

                SELECT
                    'Average Score' AS categories,
                    AVG(reputation_score) AS reputation_score,
                    AVG(total_transfer) AS total_transfer,
                    AVG(total_receive) AS total_receive,
                    AVG(in_degree) AS in_degree,
                    AVG(out_degree) AS out_degree,
                    AVG(total_volume) AS total_volume,
                    AVG(total_gas_spent) AS total_gas_spent
                FROM
                    rs_table

                UNION ALL

                SELECT
                    'Variance' AS categories,
                    VAR_SAMP(reputation_score) AS reputation_score,
                    VAR_SAMP(total_transfer) AS total_transfer,
                    VAR_SAMP(total_receive) AS total_receive,
                    VAR_SAMP(in_degree) AS in_degree,
                    VAR_SAMP(out_degree) AS out_degree,
                    VAR_SAMP(total_volume) AS total_volume,
                    VAR_SAMP(total_gas_spent) AS total_gas_spent
                FROM
                    rs_table
            ) eth_nzeoa_overview
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_rs_histogram_eoa_group = PostgresOperator(
        task_id="truncate_eth_rs_histogram_eoa_group",
        sql="""
            truncate eth_rs_histogram_eoa_group
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_rs_histogram_eoa_group',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_rs_histogram_eoa_group', 
                '{user}', 
                '{password}'
            )
            (bin_floor, bin_category, count_log)
            SELECT bin_floor, bin_category, count_log FROM (
                WITH rs_table AS (
                    SELECT *
                    FROM eth_reputation_score ers
                    WHERE ers.snapshot_block_number = (SELECT MAX(snapshot_block_number) FROM eth_reputation_score)
                    AND ers.contract=False
                    AND ers.reputation_score >0
                )
                SELECT
                    CAST(FLOOR(reputation_score_log_scale / 2) * 2 AS INT) AS bin_floor,
                    LOG10(COUNT(*)) AS count_log,
                    CASE
                        WHEN CAST(FLOOR(reputation_score_log_scale / 2) * 2 AS INT) < 25 THEN '1. Lowest'
                        WHEN CAST(FLOOR(reputation_score_log_scale / 2) * 2 AS INT) > 25
                            AND CAST(FLOOR(reputation_score_log_scale / 2) * 2 AS INT) < 50 THEN '2. Low'
                        WHEN CAST(FLOOR(reputation_score_log_scale / 2) * 2 AS INT) >= 50
                            AND CAST(FLOOR(reputation_score_log_scale / 2) * 2 AS INT) < 150 THEN '3. Medium'
                        WHEN CAST(FLOOR(reputation_score_log_scale / 2) * 2 AS INT) >= 150
                            AND CAST(FLOOR(reputation_score_log_scale / 2) * 2 AS INT) < 300 THEN '4. Experience'
                        WHEN CAST(FLOOR(reputation_score_log_scale / 2) * 2 AS INT) >= 300
                            AND CAST(FLOOR(reputation_score_log_scale / 2) * 2 AS INT) < 500 THEN '5. High'
                        ELSE '6. Highest'
                    END AS bin_category
                FROM rs_table
                GROUP BY bin_floor, bin_category
            ) eth_rs_histogram_eoa_group
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_rs_distribution_nz_eoa_group = PostgresOperator(
        task_id="truncate_eth_rs_distribution_nz_eoa_group",
        sql="""
            truncate eth_rs_distribution_nz_eoa_group
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_rs_distribution_nz_eoa_group',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_rs_distribution_nz_eoa_group', 
                '{user}', 
                '{password}'
            )
            (partition, num_of_eoa, num_of_eoa_percentage, reputation_value, reputation_value_percentage)
            SELECT partition, num_of_eoa, num_of_eoa_percentage, reputation_value, reputation_value_percentage FROM (
                WITH eth_reputation_score_tmp AS (
                    SELECT *
                    FROM eth_reputation_score
                    WHERE snapshot_block_number IN (
                        SELECT MAX(snapshot_block_number)
                        FROM eth_reputation_score
                    ) AND contract = False
                    AND reputation_score > 0
                ), distribution_on_5_partitions_of_contract_group AS (
                    SELECT 
                        CASE 
                            WHEN reputation_score_log_scale > 500 THEN '6. Highest'
                            WHEN reputation_score_log_scale <= 500 AND reputation_score_log_scale > 300 THEN '5. High'
                            WHEN reputation_score_log_scale <= 300 AND reputation_score_log_scale > 150 THEN '4. Experience'
                            WHEN reputation_score_log_scale <= 150 AND reputation_score_log_scale > 50 THEN '3. Medium'
                            WHEN reputation_score_log_scale <= 50 AND reputation_score_log_scale > 25 THEN '2. Low'
                            ELSE '1. Lowest'
                        END AS partition,
                        COUNT(*) AS num_of_eoa,
                        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM eth_reputation_score_tmp), 4) AS num_of_eoa_percentage,
                        ROUND(SUM(reputation_score), 2) AS reputation_value,
                        ROUND(SUM(reputation_score) * 100.0 / (SELECT SUM(reputation_score) FROM eth_reputation_score_tmp), 2) AS reputation_value_percentage
                    FROM eth_reputation_score_tmp
                    GROUP BY partition
                )
                SELECT * FROM distribution_on_5_partitions_of_contract_group
            ) eth_rs_distribution_nz_eoa_group
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_overview_nzeoa_group_mean = PostgresOperator(
        task_id="truncate_eth_overview_nzeoa_group_mean",
        sql="""
            truncate eth_overview_nzeoa_group_mean
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_overview_nzeoa_group_mean',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_overview_nzeoa_group_mean', 
                '{user}', 
                '{password}'
            )
            (partition, reputation_score, total_transfer, total_receive, in_degree, out_degree, total_volume, total_gas_spent)
            SELECT partition, reputation_score, total_transfer, total_receive, in_degree, out_degree, total_volume, total_gas_spent FROM (
                SELECT
                    CASE
                        WHEN reputation_score_log_scale > 500 THEN '6. Highest'
                        WHEN reputation_score_log_scale <= 500 AND reputation_score_log_scale > 300 THEN '5. High'
                        WHEN reputation_score_log_scale <= 300 AND reputation_score_log_scale > 150 THEN '4. Experience'
                        WHEN reputation_score_log_scale <= 150 AND reputation_score_log_scale > 50 THEN '3. Medium'
                        WHEN reputation_score_log_scale <= 50 AND reputation_score_log_scale > 25 THEN '2. Low'
                        ELSE '1. Lowest'
                    END AS partition,
                    AVG(reputation_score) AS reputation_score,
                    AVG(total_transfer) AS total_transfer,
                    AVG(total_receive) AS total_receive,
                    AVG(in_degree) AS in_degree,
                    AVG(out_degree) AS out_degree,
                    AVG(total_volume) AS total_volume,
                    AVG(total_gas_spent) AS total_gas_spent
                FROM eth_reputation_score ers
                WHERE ers.snapshot_block_number = (
                    SELECT MAX(snapshot_block_number) FROM eth_reputation_score
                )
                    AND ers.contract = FALSE
                GROUP BY partition
            ) eth_overview_nzeoa_group_mean
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_overview_nzeoa_group_var = PostgresOperator(
        task_id="truncate_eth_overview_nzeoa_group_var",
        sql="""
            truncate eth_overview_nzeoa_group_var
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_overview_nzeoa_group_var',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_overview_nzeoa_group_var', 
                '{user}', 
                '{password}'
            )
            (partition, reputation_score, total_transfer, total_receive, in_degree, out_degree, total_volume, total_gas_spent)
            SELECT partition, reputation_score, total_transfer, total_receive, in_degree, out_degree, total_volume, total_gas_spent FROM (
                SELECT
                    CASE
                        WHEN reputation_score_log_scale > 500 THEN '6. Highest'
                        WHEN reputation_score_log_scale <= 500 AND reputation_score_log_scale > 300 THEN '5. High'
                        WHEN reputation_score_log_scale <= 300 AND reputation_score_log_scale > 150 THEN '4. Experience'
                        WHEN reputation_score_log_scale <= 150 AND reputation_score_log_scale > 50 THEN '3. Medium'
                        WHEN reputation_score_log_scale <= 50 AND reputation_score_log_scale > 25 THEN '2. Low'
                        ELSE '1. Lowest'
                    END AS partition,
                    VAR_SAMP(reputation_score) AS reputation_score,
                    VAR_SAMP(total_transfer) AS total_transfer,
                    VAR_SAMP(total_receive) AS total_receive,
                    VAR_SAMP(in_degree) AS in_degree,
                    VAR_SAMP(out_degree) AS out_degree,
                    VAR_SAMP(total_volume) AS total_volume,
                    VAR_SAMP(total_gas_spent) AS total_gas_spent
                FROM eth_reputation_score ers
                WHERE ers.snapshot_block_number = (
                    SELECT MAX(snapshot_block_number) FROM eth_reputation_score
                )
                    AND ers.contract = FALSE
                GROUP BY partition
            ) eth_overview_nzeoa_group_var
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_top10nzeoa_highest_w_uaw = PostgresOperator(
        task_id="truncate_eth_top10nzeoa_highest_w_uaw",
        sql="""
            truncate eth_top10nzeoa_highest_w_uaw
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_top10nzeoa_highest_w_uaw',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_top10nzeoa_highest_w_uaw', 
                '{user}', 
                '{password}'
            )
            (rank, address, identity, reputation_score, in_degree, out_degree, total_transfer, total_receive, total_volume, total_gas_spent, uaw, average_rs)
            SELECT rank, address, identity, reputation_score, in_degree, out_degree, total_transfer, total_receive, total_volume, total_gas_spent, uaw, average_rs FROM (
                WITH top10eoa AS (
                    SELECT
                        ers.address AS contract_address,
                        ers.rank AS grank, --rank is a function name
                        ers.*,
                        lc.identity,
                    FROM
                        eth_reputation_score ers
                    INNER JOIN label_eth_eoa lc ON lc.address = ers.address -- add identity
                    WHERE
                        ers.snapshot_block_number = (
                            SELECT MAX(snapshot_block_number) FROM eth_reputation_score
                        )
                        AND
                        ers.contract = FALSE
                    LIMIT
                        10
                ), combined_table AS (
                    SELECT DISTINCT
                        et.to_address AS contract_address,
                        et.from_address AS interact_address,
                        rs_table.RS AS RS
                    FROM
                        eth_transaction et
                    INNER JOIN (
                        SELECT
                            address,
                            reputation_score AS RS,
                            contract
                        FROM
                            eth_reputation_score
                        WHERE
                            snapshot_block_number = (SELECT MAX(snapshot_block_number) FROM eth_reputation_score) -- make sure RS is lastest, avoid duplicate with old RS
                            AND
                            contract = FALSE 
                            AND
                            RS <= 651.3227399106263
                    ) rs_table
                    ON et.from_address = rs_table.address
                    WHERE
                        et.to_address IN (SELECT contract_address FROM top10eoa)
                        AND et.block_number BETWEEN 15993179 AND (SELECT MAX(snapshot_block_number) FROM eth_reputation_score)
                ), top10_uaw AS (
                    SELECT
                        tc.contract_address AS contract_address,
                        COUNT(ct.interact_address) AS UAW,
                        AVG(ct.RS) AS avg_RS
                    FROM combined_table ct
                    INNER JOIN top10eoa tc ON tc.contract_address = ct.contract_address
                    GROUP BY
                        tc.contract_address
                )
                SELECT
                    grank AS rank,
                    tc.contract_address AS address,
                    tc.identity as identity,
                    tc.reputation_score AS reputation_score,
                    tc.in_degree AS in_degree,
                    tc.out_degree AS out_degree,
                    tc.total_transfer AS total_transfer,
                    tc.total_receive AS total_receive,
                    tc.total_volume AS total_volume,
                    tc.total_gas_spent AS total_gas_spent,
                    UAW as uaw,
                    avg_RS AS average_rs
                FROM
                    top10_uaw tu
                INNER JOIN
                    top10eoa tc ON tu.contract_address = tc.contract_address
            ) eth_top10nzeoa_highest_w_uaw
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_top10nzeoa_high = PostgresOperator(
        task_id="truncate_eth_top10nzeoa_high",
        sql="""
            truncate eth_top10nzeoa_high
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_top10nzeoa_high',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_top10nzeoa_high', 
                '{user}', 
                '{password}'
            )
            (rank, address, identity, reputation_score, in_degree, out_degree, total_transfer, total_receive, total_volume, total_gas_spent)
            SELECT rank, address, identity, reputation_score, in_degree, out_degree, total_transfer, total_receive, total_volume, total_gas_spent FROM (
                WITH rs_table AS (
                    SELECT *
                    FROM eth_reputation_score ers
                    WHERE ers.contract = FALSE AND ers.reputation_score > 0
                    AND ers.snapshot_block_number = (SELECT MAX(snapshot_block_number) FROM eth_reputation_score)
                    AND ers.address not in ( -- quick fix due to lacking contract
                    '0xce99861f0244d6478a309e2c2565fdaaaa8b0bfb',
                    '0xf2e2237b74f0615bb18f265254cc2fbb1a229574',
                    '0xfe1ab53b234fcf9a5252e0b73631a143797d9c3b',
                    '0x1c70d0a86475cc707b48aa79f112857e7957274f',
                    '0xa3728dfd8b471e5ead8b6f08cc37d139b23fe5a9')
                )
                SELECT 
                    rank as rank,
                    address as address,
                    COALESCE(identity, 'Unknown') as identity,
                    reputation_score as reputation_score,
                    in_degree as in_degree,
                    out_degree as out_degree,
                    total_transfer as total_transfer,
                    total_receive as total_receive,
                    total_volume as total_volume,
                    total_gas_spent as total_gas_spent
                FROM rs_table
                LEFT JOIN label_eth_eoa lt ON lt.address = rs_table.address
                WHERE rs_table.reputation_score_log_scale < 500 AND rs_table.reputation_score_log_scale >=300
                ORDER BY rank
                LIMIT 10
            ) eth_top10nzeoa_high
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_top10nzeoa_experience = PostgresOperator(
        task_id="truncate_eth_top10nzeoa_experience",
        sql="""
            truncate eth_top10nzeoa_experience
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_top10nzeoa_experience',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_top10nzeoa_experience', 
                '{user}', 
                '{password}'
            )
            (rank, address, identity, reputation_score, in_degree, out_degree, total_transfer, total_receive, total_volume, total_gas_spent)
            SELECT rank, address, identity, reputation_score, in_degree, out_degree, total_transfer, total_receive, total_volume, total_gas_spent FROM (
                WITH rs_table AS (
                    SELECT *
                    FROM eth_reputation_score ers
                    WHERE ers.contract = FALSE AND ers.reputation_score > 0
                    AND ers.snapshot_block_number = (SELECT MAX(snapshot_block_number) FROM eth_reputation_score)
                )
                SELECT 
                    rank as rank,
                    address as address,
                    COALESCE(identity, 'Unknown') as identity,
                    reputation_score as reputation_score,
                    in_degree as in_degree,
                    out_degree as out_degree,
                    total_transfer as total_transfer,
                    total_receive as total_receive,
                    total_volume as total_volume,
                    total_gas_spent as total_gas_spent
                FROM rs_table
                LEFT JOIN label_eth_eoa lt ON lt.address = rs_table.address
                WHERE rs_table.reputation_score_log_scale < 300 AND rs_table.reputation_score_log_scale >=150
                ORDER BY rank
                LIMIT 10
            ) eth_top10nzeoa_experience
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_top10nzeoa_medium = PostgresOperator(
        task_id="truncate_eth_top10nzeoa_medium",
        sql="""
            truncate eth_top10nzeoa_medium
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_top10nzeoa_medium',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_top10nzeoa_medium', 
                '{user}', 
                '{password}'
            )
            (rank, address, reputation_score, in_degree, out_degree, total_transfer, total_receive, total_volume, total_gas_spent)
            SELECT rank, address, reputation_score, in_degree, out_degree, total_transfer, total_receive, total_volume, total_gas_spent FROM (
                WITH rs_table AS (
                    SELECT *
                    FROM eth_reputation_score ers
                    WHERE ers.contract = FALSE AND ers.reputation_score > 0
                    AND ers.snapshot_block_number = (SELECT MAX(snapshot_block_number) FROM eth_reputation_score)
                )
                SELECT 
                    rank as rank,
                    address as address,
                    reputation_score as reputation_score,
                    in_degree as in_degree,
                    out_degree as out_degree,
                    total_transfer as total_transfer,
                    total_receive as total_receive,
                    total_volume as total_volume,
                    total_gas_spent as total_gas_spent
                FROM rs_table
                LEFT JOIN label_eth_eoa lt ON lt.address = rs_table.address
                WHERE rs_table.reputation_score_log_scale < 150 AND rs_table.reputation_score_log_scale >=50
                ORDER BY rank
                LIMIT 10
            ) eth_top10nzeoa_medium
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_top10nzeoa_low = PostgresOperator(
        task_id="truncate_eth_top10nzeoa_low",
        sql="""
            truncate eth_top10nzeoa_low
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_top10nzeoa_low',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_top10nzeoa_low', 
                '{user}', 
                '{password}'
            )
            (rank, address, reputation_score, in_degree, out_degree, total_transfer, total_receive, total_volume, total_gas_spent)
            SELECT rank, address, reputation_score, in_degree, out_degree, total_transfer, total_receive, total_volume, total_gas_spent FROM (
                WITH rs_table AS (
                    SELECT *
                    FROM eth_reputation_score ers
                    WHERE ers.contract = FALSE AND ers.reputation_score > 0
                    AND ers.snapshot_block_number = (SELECT MAX(snapshot_block_number) FROM eth_reputation_score)
                )
                SELECT 
                    rank as rank,
                    address as address,
                    reputation_score as reputation_score,
                    in_degree as in_degree,
                    out_degree as out_degree,
                    total_transfer as total_transfer,
                    total_receive as total_receive,
                    total_volume as total_volume,
                    total_gas_spent as total_gas_spent
                FROM rs_table
                LEFT JOIN label_eth_eoa lt ON lt.address = rs_table.address
                WHERE rs_table.reputation_score_log_scale < 50 AND rs_table.reputation_score_log_scale >=25
                ORDER BY rank
                LIMIT 10
            ) eth_top10nzeoa_low
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_top10nzeoa_lowest = PostgresOperator(
        task_id="truncate_eth_top10nzeoa_lowest",
        sql="""
            truncate eth_top10nzeoa_lowest
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_top10nzeoa_lowest',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_top10nzeoa_lowest', 
                '{user}', 
                '{password}'
            )
            (rank, address, reputation_score, in_degree, out_degree, total_transfer, total_receive, total_volume, total_gas_spent)
            SELECT rank, address, reputation_score, in_degree, out_degree, total_transfer, total_receive, total_volume, total_gas_spent FROM (
                WITH rs_table AS (
                    SELECT *
                    FROM eth_reputation_score ers
                    WHERE ers.contract = FALSE AND ers.reputation_score > 0
                    AND ers.snapshot_block_number = (SELECT MAX(snapshot_block_number) FROM eth_reputation_score)
                )
                SELECT 
                    rank as rank,
                    address as address,
                    reputation_score as reputation_score,
                    in_degree as in_degree,
                    out_degree as out_degree,
                    total_transfer as total_transfer,
                    total_receive as total_receive,
                    total_volume as total_volume,
                    total_gas_spent as total_gas_spent
                FROM rs_table
                LEFT JOIN label_eth_eoa lt ON lt.address = rs_table.address
                WHERE rs_table.reputation_score_log_scale < 25 AND rs_table.reputation_score_log_scale > 0
                ORDER BY rank
                LIMIT 10
            ) eth_top10nzeoa_lowest
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_historical_eoa = PostgresOperator(
        task_id="truncate_eth_historical_eoa",
        sql="""
            truncate eth_historical_eoa
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_historical_eoa',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_historical_eoa', 
                '{user}', 
                '{password}'
            )
            (address, identity, grank, time)
            SELECT address, identity, grank, time FROM (
                WITH label_eth_eoa_tmp AS (
                    SELECT *
                    FROM label_eth_eoa
                    WHERE identity NOT LIKE 'Unknow%'
                )
                SELECT
                    lt.address AS address,
                    lt.identity as identity,
                    es.rank AS grank,
                    es.snapshot_block_timestamp AS time
                FROM label_eth_eoa_tmp lt
                LEFT JOIN
                (SELECT * FROM eth_reputation_score WHERE rank < 50) es
                ON lt.address = es.address
                WHERE rank > 0
            ) eth_historical_eoa
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_historical_contract = PostgresOperator(
        task_id="truncate_eth_historical_contract",
        sql="""
            truncate eth_historical_contract
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_historical_contract',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_historical_contract', 
                '{user}', 
                '{password}'
            )
            (address, identity, grank, time)
            SELECT address, identity, grank, time FROM (
                WITH label_eth_eoa_tmp AS (
                    SELECT *
                    FROM label_eth_contract
                    WHERE identity NOT LIKE 'Unknow%'
                )
                SELECT
                    lt.address AS address,
                    lt.identity as identity,
                    es.rank AS grank,
                    es.snapshot_block_timestamp AS time
                FROM label_eth_eoa_tmp lt
                LEFT JOIN
                (SELECT * FROM eth_reputation_score WHERE rank < 30) es
                ON lt.address = es.address
                WHERE rank > 0
            ) eth_historical_contract
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_uaw_cat_daily_defi = PostgresOperator(
        task_id="truncate_eth_uaw_cat_daily_defi",
        sql="""
            truncate eth_uaw_cat_daily_defi
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_uaw_cat_daily_defi',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_uaw_cat_daily_defi', 
                '{user}', 
                '{password}'
            )
            (transaction_date, uaw, transactions, average_rs, average_rs_excluded_highest)
            SELECT transaction_date, uaw, transactions, average_rs, average_rs_excluded_highest FROM (
                WITH middle_table AS ( -- middle table contain daily average RS and number of txn for each from-address
                    SELECT
                    et.from_address as address, 
                    COUNT(et.hash) as transaction_count,
                    toDate(et.block_timestamp) as transaction_date, -- group by date first time
                    avg(ers.reputation_score) as avg_rs_each_eoa, -- not the final average RS per date
                    avg(ers2.reputation_score) as avg_rs2_each_eoa -- not the final average RS per date
                    FROM
                    eth_transaction AS et
                    INNER JOIN (
                    SELECT
                        address,
                        category
                    FROM
                        label_eth_contract
                    WHERE
                        category = 'DeFi'
                    ) AS lec ON et.to_address = lec.address -- RS set all
                    INNER JOIN (
                    SELECT
                        address,
                        reputation_score,
                    FROM
                        eth_reputation_score
                    WHERE
                        contract = FALSE
                        AND snapshot_block_number = (
                        SELECT MAX(snapshot_block_number) FROM eth_reputation_score
                        )
                    ) AS ers ON et.from_address = ers.address -- RS set exlcuded highets
                    INNER JOIN (
                    SELECT
                        address,
                        reputation_score,
                    FROM
                        eth_reputation_score
                    WHERE
                        contract = FALSE
                        AND snapshot_block_number = (
                        SELECT MAX(snapshot_block_number) FROM eth_reputation_score
                        )
                        AND reputation_score_log_scale < 500 -- highest limit
                    ) AS ers2 ON et.from_address = ers2.address
                    WHERE
                    toDate(et.block_timestamp) >= '2023-01-01'
                    GROUP BY
                    et.from_address, 
                    transaction_date
                ) SELECT --aggregate from middle table to get the final one
                    transaction_date,
                    count(address) as uaw,
                    sum(transaction_count) as transactions,
                    avg(avg_rs_each_eoa) as average_rs,
                    avg(avg_rs2_each_eoa) as average_rs_excluded_highest
                    FROM
                        middle_table
                    GROUP BY
                        transaction_date
            ) eth_uaw_cat_daily_defi
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_uaw_cat_daily_nft = PostgresOperator(
        task_id="truncate_eth_uaw_cat_daily_nft",
        sql="""
            truncate eth_uaw_cat_daily_nft
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_uaw_cat_daily_nft',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_uaw_cat_daily_nft', 
                '{user}', 
                '{password}'
            )
            (transaction_date, uaw, transactions, average_rs, average_rs_excluded_highest)
            SELECT transaction_date, uaw, transactions, average_rs, average_rs_excluded_highest FROM (
                WITH middle_table AS ( -- middle table contain daily average RS and number of txn for each from-address
                    SELECT
                    et.from_address as address, 
                    COUNT(et.hash) as transaction_count,
                    toDate(et.block_timestamp) as transaction_date, -- group by date first time
                    avg(ers.reputation_score) as avg_rs_each_eoa, -- not the final average RS per date
                    avg(ers2.reputation_score) as avg_rs2_each_eoa -- not the final average RS per date
                    FROM
                    eth_transaction AS et
                    INNER JOIN (
                    SELECT
                        address,
                        category
                    FROM
                        label_eth_contract
                    WHERE
                        category = 'NFT'
                    ) AS lec ON et.to_address = lec.address -- RS set all
                    INNER JOIN (
                    SELECT
                        address,
                        reputation_score,
                    FROM
                        eth_reputation_score
                    WHERE
                        contract = FALSE
                        AND snapshot_block_number = (
                        SELECT MAX(snapshot_block_number) FROM eth_reputation_score
                        )
                    ) AS ers ON et.from_address = ers.address -- RS set exlcuded highets
                    INNER JOIN (
                    SELECT
                        address,
                        reputation_score,
                    FROM
                        eth_reputation_score
                    WHERE
                        contract = FALSE
                        AND snapshot_block_number = (
                        SELECT MAX(snapshot_block_number) FROM eth_reputation_score
                        )
                        AND reputation_score_log_scale < 500 -- highest limit
                    ) AS ers2 ON et.from_address = ers2.address
                    WHERE
                    toDate(et.block_timestamp) >= '2023-01-01'
                    GROUP BY
                    et.from_address, 
                    transaction_date
                ) SELECT --aggregate from middle table to get the final one
                    transaction_date,
                    count(address) as uaw,
                    sum(transaction_count) as transactions,
                    avg(avg_rs_each_eoa) as average_rs,
                    avg(avg_rs2_each_eoa) as average_rs_excluded_highest
                    FROM
                        middle_table
                    GROUP BY
                        transaction_date
            ) eth_uaw_cat_daily_nft
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_uaw_cat_daily_socialfi = PostgresOperator(
        task_id="truncate_eth_uaw_cat_daily_socialfi",
        sql="""
            truncate eth_uaw_cat_daily_socialfi
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_uaw_cat_daily_socialfi',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_uaw_cat_daily_socialfi', 
                '{user}', 
                '{password}'
            )
            (transaction_date, uaw, transactions, average_rs, average_rs_excluded_highest)
            SELECT transaction_date, uaw, transactions, average_rs, average_rs_excluded_highest FROM (
                WITH middle_table AS ( -- middle table contain daily average RS and number of txn for each from-address
                    SELECT
                    et.from_address as address, 
                    COUNT(et.hash) as transaction_count,
                    toDate(et.block_timestamp) as transaction_date, -- group by date first time
                    avg(ers.reputation_score) as avg_rs_each_eoa, -- not the final average RS per date
                    avg(ers2.reputation_score) as avg_rs2_each_eoa -- not the final average RS per date
                    FROM
                    eth_transaction AS et
                    INNER JOIN (
                    SELECT
                        address,
                        category
                    FROM
                        label_eth_contract
                    WHERE
                        category = 'SocialFi'
                    ) AS lec ON et.to_address = lec.address -- RS set all
                    INNER JOIN (
                    SELECT
                        address,
                        reputation_score,
                    FROM
                        eth_reputation_score
                    WHERE
                        contract = FALSE
                        AND snapshot_block_number = (
                        SELECT MAX(snapshot_block_number) FROM eth_reputation_score
                        )
                    ) AS ers ON et.from_address = ers.address -- RS set exlcuded highets
                    INNER JOIN (
                    SELECT
                        address,
                        reputation_score,
                    FROM
                        eth_reputation_score
                    WHERE
                        contract = FALSE
                        AND snapshot_block_number = (
                        SELECT MAX(snapshot_block_number) FROM eth_reputation_score
                        )
                        AND reputation_score_log_scale < 500 -- highest limit
                    ) AS ers2 ON et.from_address = ers2.address
                    WHERE
                    toDate(et.block_timestamp) >= '2023-01-01'
                    GROUP BY
                    et.from_address, 
                    transaction_date
                ) SELECT --aggregate from middle table to get the final one
                    transaction_date,
                    count(address) as uaw,
                    sum(transaction_count) as transactions,
                    avg(avg_rs_each_eoa) as average_rs,
                    avg(avg_rs2_each_eoa) as average_rs_excluded_highest
                    FROM
                        middle_table
                    GROUP BY
                        transaction_date
            ) eth_uaw_cat_daily_socialfi
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_uaw_cat_daily_gamefi = PostgresOperator(
        task_id="truncate_eth_uaw_cat_daily_gamefi",
        sql="""
            truncate eth_uaw_cat_daily_gamefi
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_uaw_cat_daily_gamefi',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_uaw_cat_daily_gamefi', 
                '{user}', 
                '{password}'
            )
            (transaction_date, uaw, transactions, average_rs, average_rs_excluded_highest)
            SELECT transaction_date, uaw, transactions, average_rs, average_rs_excluded_highest FROM (
                WITH middle_table AS ( -- middle table contain daily average RS and number of txn for each from-address
                    SELECT
                    et.from_address as address, 
                    COUNT(et.hash) as transaction_count,
                    toDate(et.block_timestamp) as transaction_date, -- group by date first time
                    avg(ers.reputation_score) as avg_rs_each_eoa, -- not the final average RS per date
                    avg(ers2.reputation_score) as avg_rs2_each_eoa -- not the final average RS per date
                    FROM
                    eth_transaction AS et
                    INNER JOIN (
                    SELECT
                        address,
                        category
                    FROM
                        label_eth_contract
                    WHERE
                        category = 'GameFi'
                    ) AS lec ON et.to_address = lec.address -- RS set all
                    INNER JOIN (
                    SELECT
                        address,
                        reputation_score,
                    FROM
                        eth_reputation_score
                    WHERE
                        contract = FALSE
                        AND snapshot_block_number = (
                        SELECT MAX(snapshot_block_number) FROM eth_reputation_score
                        )
                    ) AS ers ON et.from_address = ers.address -- RS set exlcuded highets
                    INNER JOIN (
                    SELECT
                        address,
                        reputation_score,
                    FROM
                        eth_reputation_score
                    WHERE
                        contract = FALSE
                        AND snapshot_block_number = (
                        SELECT MAX(snapshot_block_number) FROM eth_reputation_score
                        )
                        AND reputation_score_log_scale < 500 -- highest limit
                    ) AS ers2 ON et.from_address = ers2.address
                    WHERE
                    toDate(et.block_timestamp) >= '2023-01-01'
                    GROUP BY
                    et.from_address, 
                    transaction_date
                ) SELECT --aggregate from middle table to get the final one
                    transaction_date,
                    count(address) as uaw,
                    sum(transaction_count) as transactions,
                    avg(avg_rs_each_eoa) as average_rs,
                    avg(avg_rs2_each_eoa) as average_rs_excluded_highest
                    FROM
                        middle_table
                    GROUP BY
                        transaction_date
            ) eth_uaw_cat_daily_gamefi
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_uaw_cat_daily_l2s = PostgresOperator(
        task_id="truncate_eth_uaw_cat_daily_l2s",
        sql="""
            truncate eth_uaw_cat_daily_l2s
            """,
        postgres_conn_id="pg_connection"
    ) >> ClickHouseOperator(
        task_id='eth_uaw_cat_daily_l2s',
        database='default',
        sql=(
            '''
                INSERT INTO TABLE FUNCTION postgresql(
                '{host}:{port}', 
                '{database}', 
                'eth_uaw_cat_daily_l2s', 
                '{user}', 
                '{password}'
            )
            (transaction_date, uaw, transactions, average_rs, average_rs_excluded_highest)
            SELECT transaction_date, uaw, transactions, average_rs, average_rs_excluded_highest FROM (
                WITH middle_table AS ( -- middle table contain daily average RS and number of txn for each from-address
                    SELECT
                    et.from_address as address, 
                    COUNT(et.hash) as transaction_count,
                    toDate(et.block_timestamp) as transaction_date, -- group by date first time
                    avg(ers.reputation_score) as avg_rs_each_eoa, -- not the final average RS per date
                    avg(ers2.reputation_score) as avg_rs2_each_eoa -- not the final average RS per date
                    FROM
                    eth_transaction AS et
                    INNER JOIN (
                    SELECT
                        address,
                        category
                    FROM
                        label_eth_contract
                    WHERE
                        category = 'L2s'
                    ) AS lec ON et.to_address = lec.address -- RS set all
                    INNER JOIN (
                    SELECT
                        address,
                        reputation_score,
                    FROM
                        eth_reputation_score
                    WHERE
                        contract = FALSE
                        AND snapshot_block_number = (
                        SELECT MAX(snapshot_block_number) FROM eth_reputation_score
                        )
                    ) AS ers ON et.from_address = ers.address -- RS set exlcuded highets
                    INNER JOIN (
                    SELECT
                        address,
                        reputation_score,
                    FROM
                        eth_reputation_score
                    WHERE
                        contract = FALSE
                        AND snapshot_block_number = (
                        SELECT MAX(snapshot_block_number) FROM eth_reputation_score
                        )
                        AND reputation_score_log_scale < 500 -- highest limit
                    ) AS ers2 ON et.from_address = ers2.address
                    WHERE
                    toDate(et.block_timestamp) >= '2023-01-01'
                    GROUP BY
                    et.from_address, 
                    transaction_date
                ) SELECT --aggregate from middle table to get the final one
                    transaction_date,
                    count(address) as uaw,
                    sum(transaction_count) as transactions,
                    avg(avg_rs_each_eoa) as average_rs,
                    avg(avg_rs2_each_eoa) as average_rs_excluded_highest
                    FROM
                        middle_table
                    GROUP BY
                        transaction_date
            ) eth_uaw_cat_daily_l2s
            '''.format(
                host=conn_host,
                port=conn_port,
                database=conn_schema,
                user=conn_login,
                password=conn_password
            )
        ),
        clickhouse_conn_id="clickhouse_conn"
    )

    eth_global_reputation_score_overview_mean >> \
        eth_global_reputation_score_overview_var >> \
        eth_key_metric >> \
        eth_rs_all >> \
        eth_rs_all_log >> \
        eth_rs_contract_log >> \
        eth_rs_eoa >> \
        eth_rs_eoa_log >> \
        eth_main_groups >> \
        eth_rs_distribution_contract_categories >> \
        eth_average_distribution_on_5_partitions_of_contract_group >> \
        eth_variance_distribution_on_5_partitions_of_contract_group >> \
        eth_top10_contract_uaw >> \
        eth_nzeoa_overview >> \
        eth_rs_histogram_eoa_group >> \
        eth_rs_distribution_nz_eoa_group >> \
        eth_overview_nzeoa_group_mean >> \
        eth_overview_nzeoa_group_var >> \
        eth_top10nzeoa_highest_w_uaw >> \
        eth_top10nzeoa_high >> \
        eth_top10nzeoa_experience >> \
        eth_top10nzeoa_medium >> \
        eth_top10nzeoa_low >> \
        eth_top10nzeoa_lowest >> \
        eth_historical_eoa >> \
        eth_historical_contract >> \
        eth_uaw_cat_daily_defi >> \
        eth_uaw_cat_daily_nft >> \
        eth_uaw_cat_daily_socialfi >> \
        eth_uaw_cat_daily_gamefi >> \
        eth_uaw_cat_daily_l2s
