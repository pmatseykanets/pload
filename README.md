# pload

`pload` is an experimental tool for ingesting Marketo Activity feeds concurently into the DW.

## Installation

It is strongly recommended that you use a released version. Release binaries are available on the [releases](https://github.com/questex/pload/releases) page.

## Usage

```bash
$ pload -h
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

The following is an example of the default activity file in CSV format. Note that the `attributes` field is formatted as JSON.

```
marketoGUID,leadId,activityDate,activityTypeId,campaignId,primaryAttributeValueId,primaryAttributeValue, attributes
122323,6,2013-09-26T06:56:35+0000,12,11,6,Owyliphys Iledil,[{"name":"Source Type","value":"Web page visit"}]
```

You can find more information in the [Marketo documentation](http://developers.marketo.com/rest-api/bulk-extract/bulk-activity-extract/)