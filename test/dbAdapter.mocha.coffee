{expect} = require 'racer/test/util'

options =
  type: 'Mongo'
  uri: 'mongodb://localhost/test-db'

require('racer/test/dbAdapter') options, require('../src'), (run) ->

  run 'Mongo db flushing', (getStore) ->
    # TODO
