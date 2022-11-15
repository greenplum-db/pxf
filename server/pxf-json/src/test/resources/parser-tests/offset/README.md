# Generate JSON files for testing

These instructions will help you generate some of the JSON files required for testing. 

# Generate the big_data.json.bz2 file

This data set contains 30,000,000 rows of data zipped up using the BZip2 codec.
`big_data.json` and `big_data.json.bz2` were created using the following

```sql
psql
\pset tuples_only on
\o big_data.json
SELECT '{"cüstömerstätüs":"' || CASE WHEN (((i+47)%29)=0) THEN 'invälid' ELSE 'välid' END || '","name": "äää", "year": "20' || (i%89+10)::text || '", "address": "söme city", "zip": "' || (i%1000 + 90000)::text || '" },'
FROM generate_series (1,30000000)i;
```

Then go in and add the starting and ending array brackets in the data itself so that the final file looks something like this

```json
[
 {...},
 {...},
 {...},
 {...}
]i
```

then zip it up into a `BZip2` file

```sh
bzip2 -k big_data.json
```
