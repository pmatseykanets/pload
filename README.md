# pload

`pload` is an experimental tool for ingesting Marketo Activity feeds concurrently into a PostgreSQL database.

## Usage

```bash
pload -h
Usage: pload [options] [file]
  file
        A CSV file to load. If omitted read from stdin
  -c string
        Database connection string
  -i int
        Import Id
  -json
        Output results in JSON
  -m int
        Number of records per insert (default 2)
  -p int
        Max logical processors (default 1)
  -t string
        Database table to load data into (default "marketo.activities")
  -w int
        Number of workers (default 4)
  -x int
        Number of records per transaction (default 25000)
```

## Activity data

The following is an example of the activity file in CSV format. Note that the `attributes` field's value is serialized as JSON.

```
marketoGUID,leadId,activityDate,activityTypeId,campaignId,primaryAttributeValueId,primaryAttributeValue, attributes
12345,54321,2018-01-26T06:56:35+0000,12,11,6,Jhon Doe,[{"name":"Source Type","value":"Web page visit"}]
```

You can find more information in the [Marketo documentation](http://developers.marketo.com/rest-api/bulk-extract/bulk-activity-extract/)