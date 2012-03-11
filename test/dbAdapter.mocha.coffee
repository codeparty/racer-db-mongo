options =
  type: 'Mongo'
  uri: 'mongodb://localhost/test-db'

plugin = require '../src'

testRunner = require 'racer/test/dbAdapter'

testRunner options, plugin
