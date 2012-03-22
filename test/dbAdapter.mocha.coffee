{expect} = require 'racer/test/util'
racer = require 'racer/lib/racer'
shouldBehaveLikeDbAdapter= require 'racer/test/dbAdapter'

plugin = require '../src'

options =
  db:
    type: 'Mongo'
    uri: 'mongodb://localhost/test-db'

describe 'Mongo db adapter', ->
  shouldBehaveLikeDbAdapter options, [plugin]

  describe 'Mongo db flushing', ->
    beforeEach (done) ->
      for plugin in plugins
        racer.use plugin if plugin.useWith.server
      @store = racer.createStore options
      @store.flush done

    afterEach (done) ->
      @store.flush done

    it 'TODO'
