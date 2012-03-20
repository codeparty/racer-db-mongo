{EventEmitter} = require 'events'
url = require 'url'
mongo = require 'mongodb'
NativeObjectId = mongo.BSONPure.ObjectID
query = require './query'

DISCONNECTED  = 1
CONNECTING    = 2
CONNECTED     = 3
DISCONNECTING = 4

module.exports = (racer) ->
  DbMongo::Query = query racer.Promise, racer.LiveQuery
  racer.adapters.db.Mongo = DbMongo

# Examples:
# new DbMongo
#   uri: 'mongodb://localhost:port/database'
# new DbMongo
#   host: 'localhost'
#   port: 27017
#   database: 'example'
DbMongo = (options) ->
  EventEmitter.call this
  @_loadConf options if options
  @_state = DISCONNECTED
  @_collections = {}
  @_pending = []

  # TODO Make version scale beyond 1 db
  #      by sharding and with a vector
  #      clock with each member the
  #      version of a shard
  # TODO Initialize the version properly upon web server restart
  #      so it's synced with the STM server (i.e., Redis) version
  @version = undefined

  return

DbMongo:: =
  __proto__: EventEmitter::

  _loadConf: (conf) ->
    if conf.uri
      uri = url.parse conf.uri
      @_host = uri.hostname
      @_port = uri.port || 27017
      # TODO callback
      @_database = uri.pathname.replace /\//g, ''
      [@_user, @_pass] = uri.auth?.split(':') ? []
    else
      { host: @_host
      , port: @_port
      , database: @_database
      , user: @_user
      , pass: @_pass } = conf

  connect: (conf, callback) ->
    if typeof conf is 'function'
      callback = conf
    else if conf isnt undefined
      @_loadConf conf
    @_db ||= new mongo.Db(
        @_database
      , new mongo.Server @_host, @_port
    )
    @_state = CONNECTING
    @emit 'connecting'
    @_db.open (err) =>
      return callback err if err && callback
      open = =>
        @_state = CONNECTED
        @emit 'connected'
        for [method, args] in @_pending
          @[method].apply this, args
        @_pending = []

      if @_user && @_pass
        return @_db.authenticate @_user, @_pass, open
      return open()

  disconnect: (callback) ->
    collection._ready = false for _, collection of @_collections

    switch @_state
      when DISCONNECTED then callback null
      when CONNECTING
        @once 'connected', => @close callback
      when CONNECTED
        @_state = DISCONNECTING
        @_db.close()
        @_state = DISCONNECTED
        @emit 'disconnected'
        # TODO onClose callbacks for collections
        callback() if callback
      when DISCONNECTING then @once 'disconnected', -> callback null

  flush: (callback) ->
    return @_pending.push ['flush', arguments] if @_state != CONNECTED
    @_db.dropDatabase (err, done) =>
      @version = 0
      callback err, done

  # Mutator methods called via CustomDataSource::applyOps
  update: (collection, conds, op, opts, callback) ->
    @_collection(collection).update conds, op, opts, callback

  findAndModify: (collection, args...) ->
    @_collection(collection).findAndModify args...

  insert: (collection, json, opts, callback) ->
    # TODO Leverage pkey flag; it may not be _id
    toInsert = Object.create json # So we have no side-effects in tests
    toInsert._id ||= new NativeObjectId
    @_collection(collection).insert toInsert, opts, (err) ->
      return callback err if err
      callback null, {_id: toInsert._id}

  remove: (collection, conds, callback) ->
    @_collection(collection).remove conds, (err) ->
      return callback err if err

  # Callback here receives raw json data back from Mongo
  findOne: (collection, args...) ->
    @_collection(collection).findOne args...

  find: (collection, conds, opts, callback) ->
    @_collection(collection).find conds, opts, (err, cursor) ->
      return callback err if err
      cursor.toArray (err, docs) ->
        return callback err if err
        return callback null, docs

  # Finds or creates the Mongo collection
  _collection: (name) ->
    @_collections[name] ||= new Collection name, @_db

  setVersion: (ver) -> @version = Math.max @version, ver

  setupDefaultPersistenceRoutes: (store) ->
    adapter = this

    maybeCastId =
      fromDb: (objectId) ->
        return objectId.toString()
      toDb: (id) ->
        try
          return new NativeObjectId id
        catch e
          throw e unless e.message == 'Argument passed in must be a single String of 12 bytes or a string of 24 hex characters in hex format'
        return id

    store.defaultRoute 'get', '*.*.*', (collection, _id, relPath, done, next) ->
      fields = _id: 0
      if relPath == 'id' then relPath = '_id'
      fields[relPath] = 1
      adapter.findOne collection, {_id}, fields, (err, doc) ->
        return done err if err
        return done null, undefined, adapter.version unless doc

        if doc._id
          doc.id = maybeCastId.fromDb doc._id
        val = doc
        for prop in relPath.split '.'
          val = val[prop]

        done null, val, adapter.version

    store.defaultRoute 'get', '*.*', (collection, _id, done, next) ->
      adapter.findOne collection, {_id}, (err, doc) ->
        return done err if err
        return done null, undefined, adapter.version unless doc

        doc.id = maybeCastId.fromDb doc._id
        delete doc._id

        done null, doc, adapter.version

    store.defaultRoute 'set', '*.*.*', (collection, _id, relPath, val, ver, done, next) ->
      relPath = '_id' if relPath == 'id'
      (setTo = {})[relPath] = val
      op = $set: setTo
      _id = maybeCastId.toDb _id
      adapter.findAndModify collection
        , {_id}        # Conditions
        , []           # Empty sort
        , op           # Modification
        , upsert: true # If not found, insert the object rperesented by op
        , (err, origDoc) ->
          return done err if err
          adapter.setVersion ver
          if origDoc && origDoc._id
            origDoc.id = maybeCastId.fromDb origDoc._id
            delete origDoc._id
          done null, origDoc

    store.defaultRoute 'set', '*.*', (collection, id, doc, ver, done, next) ->
      cb = (err, origDoc) ->
        return done err if err
        adapter.setVersion ver
        if origDoc && origDoc._id
          origDoc.id = maybeCastId.fromDb origDoc._id
          delete origDoc._id
        done null, origDoc

      unless id
        return adapter.insert collection, doc, cb

      _id = maybeCastId.toDb id

      # Don't use `delete doc.id` so we avoid side-effects in tests
      docCopy = Object.create doc,
        id: { value: undefined, enumerable: false }
        _id: { value: _id, enumerable: true }
      adapter.findAndModify collection, {_id}, [], docCopy, upsert: true, cb

    store.defaultRoute 'del', '*.*.*', (collection, id, relPath, ver, done, next) ->
      if relPath == 'id'
        throw new Error 'Cannot delete an id'

      (unsetConf = {})[relPath] = 1
      op = $unset: unsetConf
      _id = maybeCastId.toDb id
      adapter.findAndModify collection, {_id}, [], op, (err, origDoc) ->
        return done err if err
        adapter.setVersion ver
        if origDoc && origDoc._id
          origDoc.id = maybeCastId.fromDb origDoc._id
          delete origDoc._id
        done null, origDoc

    store.defaultRoute 'del', '*.*', (collection, id, ver, done, next) ->
      _id = maybeCastId.toDb id
      adapter.findAndModify collection, {_id}, [], {}, remove: true, (err, removedDoc) ->
        return done err if err
        adapter.setVersion ver
        if removedDoc
          removedDoc.id = maybeCastId.fromDb removedDoc._id
          delete removedDoc._id
        done null, removedDoc

    store.defaultRoute 'push', '*.*.*', (collection, id, relPath, vals..., ver, done, next) ->
      op = {}
      if vals.length == 1
        (op.$push = {})[relPath] = vals[0]
      else
        (op.$pushAll = {})[relPath] = vals

      _id = maybeCastId.toDb id

      adapter.findAndModify collection, {_id}, [], op, upsert: true, (err, origDoc) ->
        if err
          if /non-array/.test err.message
            err = new Error 'Not an Array'
          return done err if err
        adapter.setVersion ver
        if origDoc && origDoc._id
          origDoc.id = maybeCastId.fromDb origDoc._id
          delete origDoc._id
        done null, origDoc

    store.defaultRoute 'unshift', '*.*.*', (collection, id, relPath, vals..., ver, done, next) ->
      fields = _id: 0
      fields[relPath] = 1
      _id = maybeCastId.toDb id
      adapter.findOne collection, {_id}, fields, (err, found) ->
        return done err if err
        arr = found?[relPath]
        arr = [] if typeof arr is 'undefined'
        return done new Error 'Not an Array' unless Array.isArray arr

        arr = vals.concat arr.slice()

        (setTo = {})[relPath] = arr
        op = $set: setTo
        adapter.findAndModify collection, {_id}, [], op, upsert: true, (err, origDoc) ->
          return done err if err
          adapter.setVersion ver
          if origDoc && origDoc._id
            origDoc.id = maybeCastId.fromDb origDoc._id
            delete origDoc._id
          done null, origDoc

    store.defaultRoute 'insert', '*.*.*', (collection, id, relPath, index, vals..., ver, done, next) ->
      fields = _id: 0
      fields[relPath] = 1
      _id = maybeCastId.toDb id
      adapter.findOne collection, {_id}, fields, (err, found) ->
        return done err if err
        arr = found?[relPath]
        arr = [] if typeof arr is 'undefined'
        return done new Error 'Not an Array' unless Array.isArray arr

        arr = arr[0...index].concat(vals).concat(arr[index..])

        (setTo = {})[relPath] = arr
        op = $set: setTo

        adapter.update collection, {_id}, op, upsert: true, (err) ->
          return done err if err
          adapter.setVersion ver
          if found
            found.id = id
            delete found._id
          done null, found

    store.defaultRoute 'pop', '*.*.*', (collection, id, relPath, ver, done, next) ->
      _id = maybeCastId.toDb id
      (popConf = {})[relPath] = 1
      op = $pop: popConf
      adapter.findAndModify collection, {_id}, [], op, (err, origDoc) ->
        if err
          if /non-array/.test err.message
            err = new Error 'Not an Array'
          return done err if err
        adapter.setVersion ver
        if origDoc && origDoc._id
          origDoc.id = maybeCastId.fromDb origDoc._id
          delete origDoc._id
        done null, origDoc

    store.defaultRoute 'shift', '*.*.*', (collection, id, relPath, ver, done, next) ->
      fields = _id: 0
      fields[relPath] = 1
      _id = maybeCastId.toDb id
      adapter.findOne collection, {_id}, fields, (err, found) ->
        return done err if err
        return done null unless found
        arr = found[relPath]
        arr = [] if typeof arr is 'undefined'
        return done new Error 'Not an Array' unless Array.isArray arr

        arr = arr.slice()
        arr.shift()

        (setTo = {})[relPath] = arr
        op = $set: setTo
        adapter.update collection, {_id}, op, (err) ->
          return done err if err
          adapter.setVersion ver
          if found
            found.id = id
            delete found._id
          done null, found

    store.defaultRoute 'remove', '*.*.*', (collection, id, relPath, index, count, ver, done, next) ->
      fields = _id: 0
      fields[relPath] = 1
      _id = maybeCastId.toDb id
      adapter.findOne collection, {_id}, fields, (err, found) ->
        return done err if err
        return done null unless found
        arr = found[relPath]
        arr = [] if typeof arr is 'undefined'
        return done new Error 'Not an Array' unless Array.isArray arr

        arr = arr.slice()
        arr.splice index, count

        (setTo = {})[relPath] = arr
        op = $set: setTo
        adapter.update collection, {_id}, op, (err) ->
          return done err if err
          adapter.setVersion ver
          found.id = maybeCastId.fromDb found._id
          delete found._id
          done null, found

    store.defaultRoute 'move', '*.*.*', (collection, id, relPath, from, to, count, ver, done, next) ->
      fields = _id: 0
      fields[relPath] = 1
      _id = maybeCastId.toDb id
      adapter.findOne collection, {_id}, fields, (err, found) ->
        return done err if err
        return done null unless found
        arr = found[relPath]
        arr = [] if typeof arr is 'undefined'
        return done new Error 'Not an Array' unless Array.isArray arr

        arr = arr.slice()
        to += arr.length if to < 0
        values = arr.splice from, count
        arr.splice to, 0, values...

        (setTo = {})[relPath] = arr
        op = $set: setTo
        adapter.update collection, {_id}, op, (err) ->
          return done err if err
          adapter.setVersion ver
          if found
            found.id = id
            delete found._id
          done null, found

MongoCollection = mongo.Collection

Collection = (name, db) ->
  @name = name
  @db = db
  @_pending = []
  @_ready = false

  db.collection name, (err, collection) =>
    throw err if err
    @_ready = true
    @collection = collection
    @onReady()

  return

Collection:: =
  onReady: ->
    for [name, args] in @_pending
      @[name].apply this, args
    @_pending = []

for name, fn of MongoCollection::
  do (name, fn) ->
    Collection::[name] = ->
      collection = @collection
      args = arguments
      if @_ready
        process.nextTick ->
          collection[name].apply collection, args
      else
        @_pending.push [name, arguments]
