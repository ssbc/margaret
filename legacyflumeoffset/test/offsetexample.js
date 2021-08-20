// SPDX-FileCopyrightText: 2021 The margaret Authors
//
// SPDX-License-Identifier: MIT

// super simple example that creates a bunch of objects and writes them to example.lfo

var OffsetLog = require('flumelog-offset')
var codec = require('flumecodec')
var Flume = require('flumedb')

var filename = 'example.lfo'
var db = Flume(OffsetLog(filename, { codec: codec.json }))

var objs = [
  'whut',
  { greets: 'hello!' },
  { test: '1' },
  true,
  { test: 2 },
  { test: 32 },
  { abc: true, more: false }
]

db.append(objs[0], function (err) {
  if (err) throw err
  db.append(objs[1], function (err) {
    if (err) throw err
    db.append(objs[2], function (err) {
      if (err) throw err
      db.append(objs[3], function (err) {
        if (err) throw err
        db.append(objs[4], function (err) {
          if (err) throw err
          db.append(objs[5], function (err) {
            if (err) throw err
            db.append(objs[6], function (err) {
              if (err) throw err
              console.log('done!')
            })
          })
        })
      })
    })
  })
})
