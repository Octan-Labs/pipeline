# Tracking L2s and LSDs

<strong>Data for compare TVL 1 week:</strong>

```javascript
{
    tvlLock1WeekAgo, // TVL 1 week ago
    tvlLockCurrent, // TVL at current timestamp
}
```

<strong>Data for compare TVL 24h:</strong>

```javascript
{
    tvlLock1WeekAgo, // TVL 24h ago
    tvlLockCurrent, // TVL at current timestamp
}
```

<strong>Data for compare TVL 1h:</strong>

```javascript
{
    tvlLock1WeekAgo, // TVL 1h ago
    tvlLockCurrent, // TVL at current timestamp
}
```

<strong>Data for chart requirements:</strong>

```javascript
// example dummy-data-chart.js
{
  USDlock, // Total USD value lock at currentTimestamp
    ETHlock, // Total ETH value lock at currentTimestamp
    currentTimestamp; // Human date -> timestamp
}
```

<strong>Data for table requirements:</strong>

```json
// example dummy-data-table.js
{
  "id": 1,
  "name": "Arbitrum One", // name of L2s
  "marketCap": 562731421761, // market cap of token L2s
  "technology": ["Optimistic Rollup", "Ethereum compatible"], // Technology used
  "tokenPrice": 1.17, // Token value of this L2s, example ARB -> 1.17$
  "reputationScore": "",
  "tvl": 5940000000, // Total value lock in this L2s, example ARB -> 5.94B
  "link": "#", // Link to chart and info of this L2s
  "marketShare": 58.16 // Market share in total TVL
}
```
