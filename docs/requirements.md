# Requirements

1. dict index address (do tràn mem)
  - input: address
  - output: 1: address

  -> lưu ở 1 table phía ngoài

2. dict block_weight(age_weight): block_number, bigint
  - input: block
  - output: block: log(params)

    -> lưu ở 1 table phía ngoài

3.a. stats jobs:
  - input: transactions + dict index address
  - output: cummulative stats

3. loop null deduplication
  - input: transactions + dict index address
  - loop: gửi theo vòng lăp
  - to_address: contract creation nên đc ignored
  - deduplication: nếu có nhiều giao dịch cùng direction cùng from/to thì đánh weight cao hơn
  - output: refined data 

4. age weight mapping:
  - input:  refined data + dict block_weight(age_weight)
  - output: refined data + age_weight column

5. personalized weights:
  - input: refined data + age_weight column
  - output: personalized weights:

6. graph init:
  - input: refined data + age_weight column
  - output: graph

7. pagerank calculation:
  - input: graph + personalized weight:
  - output: rs

8. degrees (4 cái):
  - input: graph
  - output: degrees 

Phát thảo sơ bộ:
Indexers
-> Streaming latest block: N
-> Subscriber jobs:
    -> index address job -> dict index address
      -> stats job: cummulative stats
    -> age weight mapping job :
      1-> block weight job -> dict block_weight(age_weight)
      2-> refined data job -> table defined data
        1 + 2 -> refined data + age_weight column

trigger:
-> DAG init based on a specific block N
-> chạy 4 jobs

5. personalized weights:
  - input: refined data + age_weight column
  - output: personalized weights:

6. graph init:
  - input: refined data + age_weight column
  - output: graph

7. pagerank calculation:
  - input: graph + personalized weight:
  - output: rs

8. degrees (4):
  - input: graph
  - output: degrees

