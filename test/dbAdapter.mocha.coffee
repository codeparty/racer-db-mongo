{expect} = require 'racer/test/util'

options =
  type: 'Mongo'
  uri: 'mongodb://localhost/test-db'

testRunner = require 'racer/test/dbAdapter'
plugin = require '../src'

testRunner options, plugin, (run) ->
  run 'Mongo db flushing', (getStore) ->
    # TODO
