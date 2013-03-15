{expect} = require 'racer/test/util'
racer = require 'racer'
shouldBehaveLikeDbAdapter= require 'racer/test/dbAdapter'
mongodb = require 'mongodb'
mongoskin = require 'mongoskin'
util = require '../lib/util'

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

    it 'should handle mongo created ids properly xxx', (done) ->
      db = mongoskin.db('mongodb://localhost/test-db')
      db.temp = db.collection('temp')
      modelA = @store.createModel()
      modelB = @store.createModel()

      db.temp.insert {name: 'temp'}, {safe: true}, (err, docs) ->
        expect(err).to.eql(null)
        expect(docs).to.have.length(1)

        hexString = docs[0]._id.toHexString()

        modelA.fetch "temp.#{hexString}", (err, $doc) ->
          expect($doc.get('name')).to.equal('temp')

          modelA.set "temp.#{hexString}.age", 42, (err, value) ->
            expect(err).to.eql(null)

            modelB.fetch "temp.#{hexString}", (err, $doc) ->
              expect(err).to.eql(null)
              expect($doc.get('name')).to.equal('temp')
              expect($doc.get('age')).to.equal(42)
              console.log('afterdebugger')
              modelB.del "temp.#{hexString}", (err, doc) ->

                console.log('args',arguments)
                expect(err).to.eql(null)

                modelA.fetch "temp.#{hexString}", (err, $doc) ->
                  console.log('doc', $doc)
                  expect(err).to.eql(null)
                  expect($doc.get()).to.equal(undefined)

                  db.close()
                  done()


  describe 'isObjectIdString', ->
    it 'should return true for an ObjectId string', ->
      id = new mongodb.ObjectID()
      idString = id.toHexString()
      expect(util.isObjectIdString(idString)).to.be.ok()

    it 'should return false for a racer generated id string', ->
      idString = racer.uuid()
      expect(util.isObjectIdString(idString)).to.not.be.ok()