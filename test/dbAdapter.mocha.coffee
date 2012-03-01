options =
  type: 'Mongo'
  uri: 'mongodb://localhost/test-db'

require('racer/test/util/dbAdapter') options, require('../src')
