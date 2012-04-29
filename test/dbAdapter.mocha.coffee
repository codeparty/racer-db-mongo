{expect} = require 'racer/test/util'
racer = require 'racer/lib/racer'
shouldBehaveLikeDbAdapter= require 'racer/test/dbAdapter'

plugin = require '../lib'

options =
  db:
    type: 'Mongo'
    uri: 'mongodb://localhost/test-db'

describe 'Mongo db adapter', ->
  shouldBehaveLikeDbAdapter options, [plugin]

  describe 'Mongo db flushing', ->
    beforeEach (done) ->
      racer.use plugin if plugin.useWith.server
      @store = racer.createStore options
      @store.flush done

    afterEach (done) ->
      @store.flush done

    it 'TODO'

  describe '_id and id', ->
    beforeEach (done) ->
      racer.use plugin if plugin.useWith.server
      @store = racer.createStore options
      @store.flush done

    afterEach (done) ->
      @store.flush done

    it 'should not assign an id of `null`', (done) ->
      model = @store.createModel()
      model.on 'set', =>
        @store._db.findOne 'docs', {_id: 'someId'}, {}, (err, doc) ->
          expect(err).to.not.be.ok()
          expect(doc).to.not.have.key('id')
          done()
      model.set 'docs.someId', name: 'yoyo'
