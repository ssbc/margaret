<!--
SPDX-FileCopyrightText: 2021 The margaret Authors

SPDX-License-Identifier: MIT
-->

# Margaret [![Go Reference](https://pkg.go.dev/badge/go.cryptoscope.co/margaret.svg)](https://pkg.go.dev/go.cryptoscope.co/margaret) ![[Github Actions Tests](https://github.com/cryptoscope/margaret/actions/workflows/go.yml)](https://github.com/cryptoscope/margaret/actions/workflows/go.yml/badge.svg) [![Go Report Card](https://goreportcard.com/badge/go.cryptoscope.co/margaret)](https://goreportcard.com/report/go.cryptoscope.co/margaret)
Margaret is [`go-ssb`](https://github.com/cryptoscope/ssb)'s [append-only](https://en.wikipedia.org/wiki/Append-only)
log\* provider, and greatly inspired by [flumedb](https://github.com/flumedb/flumedb). Compatible with Go 1.13+.

![margaret the log lady, 1989 edition](https://static.wikia.nocookie.net/twinpeaks/images/6/68/Logladyreplacement.jpg/revision/latest/scale-to-width-down/500?cb=20160906170235)

_the project name is inspired by Twin Peaks's character [Margaret](https://twinpeaks.fandom.com/wiki/Margaret_Lanterman) aka **the
log lady**_

Margaret has the following facilities:
* an append-only log interface `.Append(interface{})`, `.Get(int64)`
* [queries](https://github.com/cryptoscope/margaret/blob/master/qry.go) `.Query(...QuerySpec)` for retrieving ranges based on sequence numbers e.g. `Query.Gt(int64)`, or limiting the amount of data returned `.Limit(int64)` 
* a variety of index mechanisms, both for categorizing log entries into buckets and for creating virtual logs (aka sublogs)

### Log storage
Margaret outputs data according to the [`offset2`](https://godocs.io/go.cryptoscope.co/margaret/offset2) format, which is inspired by (but significantly differs from) [`flumelog-offset`](https://github.com/flumedb/flumelog-offset).

In brief: margaret stores the data of _all logs_ in the three following files:
* `data` stores the actual data (with a length-prefix before each entry)
* `ofst` indexes the starting locations for each data entry in `data`
* `jrnl` an integrity checking mechanism for all three files; a checksum of sorts, [more details](https://github.com/cryptoscope/margaret/blob/master/offset2/log.go#L215)

## More details
* multilogs, similar to leveldb indexes
* sublogs (and rxLog/receiveLog/offsetLog and its equivalence to offset.log)
* indexes
* queries
* zeroing out written data

## Components
Margaret is one of a few components that make the [go implementation of ssb](https://github.com/cryptoscope/ssb/) tick:
* [`ssb/sbot`](https://github.com/cryptoscope/ssb/) uses margaret for storing each peer's data
* [`luigi`](https://github.com/cryptoscope/luigi) is used by margaret to pipe data (in the form of [`muxrpc`](https://github.com/cryptoscope/go-muxrpc) responses) from connected scuttlebutt peers into storage on the local machine


\* margaret is technically an append-_based_ log, as there is support for
zeroing out items in the log after they have already been written. Given
the relative ubiquity of append-only logs & their uses, it's easier to just say
append-only log.
