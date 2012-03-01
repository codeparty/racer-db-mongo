options =
  type: 'Mongo'
  uri: 'mongodb://localhost/test-db'

require('racer/test/dbAdapter') options, require('../src')
