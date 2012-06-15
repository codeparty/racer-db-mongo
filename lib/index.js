var EventEmitter    = require('events').EventEmitter
  , url             = require('url')
  , mongo           = require('mongodb')
  , NativeObjectId  = mongo.BSONPure.ObjectID
  , createQueryCtor = require('./query')

  , DISCONNECTED  = 1
  , CONNECTING    = 2
  , CONNECTED     = 3
  , DISCONNECTING = 4;

exports = module.exports = plugin;

function plugin (racer) {
  DbMongo.prototype.Query = createQueryCtor(racer);
  racer.registerAdapter('db', 'Mongo', DbMongo);
}

// Make this a racer plugin. This tells `derby.use(...)` to use the plugin on
// racer, not derby.
exports.decorate = 'racer';

exports.useWith = { server: true, browser: false };

// Examples:
// new DbMongo({uri: 'mongodb://localhost:port/database'});
// new DbMongo({
//     host: 'localhost'
//   , port: 27017
//   , database: 'example'
// });
function DbMongo (options) {
  EventEmitter.call(this);
  if (options) this._loadConf(options);
  this._state = DISCONNECTED;
  this._collections = {};

  // For storing pending method calls prior to connection
  this._pending = [];

  // TODO Make version scale beyond 1 db
  //      by sharding and with a vector
  //      clock with each member the
  //      version of a shard
  // TODO Initialize the version properly upon web server restart
  //      so it's synced with the STM server (i.e., Redis) version
  this.version = undefined;
}

DbMongo.prototype.__proto__ = EventEmitter.prototype;

DbMongo.prototype._loadConf = function loadConf (conf) {
  if (conf.uri) {
    var uri = url.parse(conf.uri);
    this._host = uri.hostname;
    this._port = parseInt(uri.port || 27017, 10);
    // TODO callback
    this._database = uri.pathname.replace(/\//g, '');

    var auth = uri.auth;
    if (auth) {
      var authPair = auth.split(':');
      if (authPair) {
        this._user = authPair[0];
        this._pass = authPair[1];
      }
    }
  } else {
    this._host     = conf.host;
    this._port     = conf.port;
    this._database = conf.database || conf.db;
    this._user     = conf.user;
    this._pass     = conf.pass;
  }
};

DbMongo.prototype.connect = function connect (conf, cb) {
  if (typeof conf === 'function') {
    cb = conf;
  } else if (typeof conf !== 'undefined') {
    this._loadConf(conf);
  }
  var db = this._db;
  if (! db) {
    var mongoServer = new mongo.Server(this._host, this._port);
    db = this._db = new mongo.Db(this._database, mongoServer);
  }
  this._state = CONNECTING;
  this.emit('connecting');
  var self = this;
  db.open( function (err) {
    if (err && cb) return cb(err);

    function open () {
      self._state = CONNECTED;
      self.emit('connected');
      var pending = self._pending
        , currPending, method, args;
      while(currPending = pending.shift()) {
        method = currPending[0];
        args   = currPending[1];
        self[method].apply(self, args);
      }
    }

    if (self._user && self._pass) {
      return db.authenticate(self._user, self._pass, open);
    }
    return open();
  });
};

DbMongo.prototype.disconnect = function disconnect (cb) {
  var collections = this._collections
    , coll;
  for (var k in collections) {
    collections[k]._ready = false;
  }
  var self = this;
  switch (this._state) {
    case DISCONNECTED: callback(null); break;
    case CONNECTING:
      this.once('connected', function () { self.close(cb); });
      break;
    case CONNECTED:
      this._state = DISCONNECTING;
      this.emit('disconnecting');
      this._db.close();
      this._state = DISCONNECTED;
      this.emit('disconnected');
      // TODO onClose callbacks for collections
      if (cb) cb();
      break;
    case DISCONNECTING: this.once('disconnected', cb); break;
  }
};

DbMongo.prototype.flush = function flush (cb) {
  if (this._state !== CONNECTED) {
    return this._pending.push(['flush', arguments]);
  }

  var self = this;
  this._db.dropDatabase( function dropDatabaseCb (err, done) {
    self.version = 0;
    cb(err, done);
  });
};

// Mutator methods called via CustomDataSource.prototype.applyOps
DbMongo.prototype.update = function update (collection, conds, op, opts, cb) {
  this._collection(collection).update(conds, op, opts, cb);
};

DbMongo.prototype.findAndModify = function findAndModify (collection) {
  var args = Array.prototype.slice.call(arguments, 1);
  var coll = this._collection(collection);
  coll.findAndModify.apply(coll, args);
}

DbMongo.prototype.insert = function insert (collection, json, opts, cb) {
  // TODO Leverage pkey flag; it may not be _id
  var toInsert = Object.create(json);
  toInsert._id || (toInsert._id = new NativeObjectId);
  var coll = this._collection(collection);
  coll.insert(toInsert, opts, function insertCb (err) {
    if (err) return cb(err);
    cb(null, {_id: toInsert._id});
  });
};

DbMongo.prototype.remove = function remove (collection, conds, cb) {
  this._collection(collection).remove(conds, cb);
};

DbMongo.prototype.findOne = function findOne (collection) {
  var args = Array.prototype.slice.call(arguments, 1);
  var coll = this._collection(collection);
  coll.findOne.apply(coll, args);
};

DbMongo.prototype.find = function find (collection, conds, opts, cb) {
  this._collection(collection).find(conds, opts, function findCb (err, cursor) {
    if (err) return cb(err);
    cursor.toArray( function toArrayCb (err, docs) {
      if (err) return cb(err);
      return cb(null, docs);
    });
  });
};

DbMongo.prototype._collection = function _collection (name) {
  return this._collections[name] || (
    this._collections[name] = new Collection(name, this._db)
  );
};

DbMongo.prototype.setVersion = function setVersion (ver) {
  this.version = Math.max(this.version, ver);
};

DbMongo.prototype.setupRoutes = function setupRoutes (store) {
  var adapter = this;
  var maybeCastId = {
      fromDb: function fromDb (objectId) { return objectId.toString(); }
    , toDb: function toDb (id) {
        if (typeof id === 'number' || (id.length !== 12 && id.length !== 24)) {
          return id;
        }
        return new NativeObjectId(id);
      }
  };

  store.route('get', '*.*.*', -1000, function (collection, _id, relPath, done, next) {
    var fields = { _id: 0 };
    if (relPath === 'id') relPath = '_id';
    fields[relPath] = 1;
    adapter.findOne(collection, {_id: _id}, fields, function findOneCb (err, doc) {
      if (err) return done(err);
      if (!doc) return done(null, undefined, adapter.version);
      doc.id = _id;
      var curr = doc;
      var parts = relPath.split('.');
      for (var i = 0, l = parts.length; i < l; i++) {
        curr = curr[parts[i]];
      }
      done(null, curr, adapter.version);
    });
  });

  store.route('get', '*.*', -1000, function (collection, _id, done, next) {
    adapter.findOne(collection, {_id: _id}, function findOneCb (err, doc) {
      if (err) return done(err);
      if (!doc) return done(null, undefined, adapter.version);
      doc.id = _id;
      delete doc._id
      done(null, doc, adapter.version);
    });
  });

  store.route('get', '*', -1000, function (collection, done, next) {
    adapter.find(collection, {}, {}, function findCb (err, docList) {
      if (err) return done(err);
      var docs = {}, doc;
      for (var i = docList.length; i--; ) {
        doc = docList[i];
        docs[doc._id] = doc;
      }
      return done(null, docs, adapter.version);
    });
  });

  function createCb (ver, done) {
    return function findAndModifyCb (err, origDoc) {
      if(err) return done(err);
      adapter.setVersion(ver);
      if (origDoc && origDoc._id) {
        origDoc.id = maybeCastId.fromDb(origDoc._id);
        delete origDoc._id;
      }
      done(null, origDoc);
    };
  }

  store.route('set', '*.*.*', -1000, function setCb (collection, _id, relPath, val, ver, done, next) {
    if (relPath === 'id') relPath = '_id';
    var setTo = {};
    setTo[relPath] = val;
    var op = { $set: setTo };
    // TODO Don't let sessions leak into this abstraction
    if (collection !== 'sessions') {
      _id = maybeCastId.toDb(_id);
    }
    adapter.findAndModify(collection
      , {_id: _id}     // Conditions
      , []             // Empty sort
      , op             // Modification
      , {upsert: true} // If not found, insert the object represented by op
      , createCb(ver, done)
    );
  });

  store.route('set', '*.*', -1000, function (collection, id, doc, ver, done, next) {
    var findAndModifyCb = createCb(ver, done);

    if (!id) {
      return adapter.insert(collection, doc, cb);
    }

    var _id = maybeCastId.toDb(id);

    // Don't use `delete doc.id` so we avoid side-effects in tests
    var docCopy = {};
    for (var k in doc) {
      if (k === 'id') docCopy._id = _id
      else docCopy[k] = doc[k];
    }
    adapter.findAndModify(collection, {_id: _id}, [], docCopy, {upsert: true}, createCb(ver, done));
  });

  store.route('del', '*.*.*', -1000, function delCb (collection, id, relPath, ver, done, next) {
    if (relPath === 'id') {
      throw new Error('Cannot delete an id');
    }

    var unsetConf = {};
    unsetConf[relPath] = 1;
    var op = { $unset: unsetConf };
    var _id = maybeCastId.toDb(id);
    var findAndModifyCb = createCb(ver, done);
    adapter.findAndModify(collection, {_id: _id}, [], op, findAndModifyCb);
  });

  store.route('del', '*.*', -1000, function delCb (collection, id, ver, done, next) {
    var _id = maybeCastId.toDb(id);
    adapter.findAndModify(collection, {_id: _id}, [], {}, {remove: true}, createCb(ver, done));
  });

  function createPushPopFindAndModifyCb (ver, done) {
    return function findAndModifyCb (err, origDoc) {
      if (err) {
        if (/non-array/.test(err.message)) {
          err = new Error('Not an Array');
        }
        if (err) return done(err);
      }
      createCb(ver, done)(err, origDoc);
    }
  }

  // pushCb(collection, id, relPath, vals..., ver, done, next);
  store.route('push', '*.*.*', -1000, function pushCb (collection, id, relPath) {
    var arglen = arguments.length;
    var vals = Array.prototype.slice.call(arguments, 3, arglen-3);
    var ver  = arguments[arglen-3]
    var done = arguments[arglen-2];
    var next = arguments[arglen-1];
    var op = {};
    if (vals.length === 1) (op.$push = {})[relPath] = vals[0];
    else (op.$pushAll = {})[relPath] = vals;

    var _id = maybeCastId.toDb(id);

    adapter.findAndModify(collection, {_id: _id}, [], op, {upsert: true}, createPushPopFindAndModifyCb(ver, done));
  });

  store.route('pop', '*.*.*', -1000, function popCb (collection, id, relPath, ver, done, next) {
    var _id = maybeCastId.toDb(id, collection);
    var popConf = {};
    popConf[relPath] = 1;
    var op = { $pop: popConf };
    adapter.findAndModify(collection, {_id: _id}, [], op, {upsert: true}, createPushPopFindAndModifyCb(ver, done));
  });

  function createFindOneCb (collection, _id, relPath, ver, done, extra, genNewArray) {
    if (arguments.length === createFindOneCb.length-1) {
      genNewArray = extra;
      extra = null;
    }
    return function cb (err, found) {
      if (err) return done(err);
//      if (!found) {
//        return done(null);
//      }
      var arr = found && found[relPath];
      if (!arr) arr = [];
      if (! Array.isArray(arr)) {
        return done(new Error('Not an Array'));
      }

      arr = genNewArray(arr, extra);

      var setTo = {};
      setTo[relPath] = arr;

      var op = { $set: setTo };

      adapter.findAndModify(collection, {_id: _id}, [], op, {upsert: true}, createCb(ver, done));
    };
  }

  // unshiftCb(collection, id, relPath, vals..., ver, done, next);
  store.route('unshift', '*.*.*', -1000, function unshiftCb (collection, id, relPath) {
    var arglen = arguments.length;
    var vals = Array.prototype.slice.call(arguments, 3, arglen-3);
    var ver = arguments[arglen-3];
    var done = arguments[arglen-2];
    var next = arguments[arglen-1];

    var fields = {_id: 0};
    fields[relPath] = 1;
    var _id = maybeCastId.toDb(id);
    var cb = createFindOneCb(collection, _id, relPath, ver, done, {vals: vals}, function (arr, extra) {
      return extra.vals.concat(arr.slice());
    });
    adapter.findOne(collection, {_id: _id}, fields, cb);
  });

  store.route('insert', '*.*.*', -1000, function insertCb (collection, id, relPath, index) {
    var arglen = arguments.length;
    var vals = Array.prototype.slice.call(arguments, 4, arglen-3);
    var ver = arguments[arglen-3];
    var done = arguments[arglen-2];
    var next = arguments[arglen-1];

    var fields = {_id: 0};
    fields[relPath] = 1;
    var _id = maybeCastId.toDb(id);
    var cb = createFindOneCb(collection, _id, relPath, ver, done, {vals: vals, index: index}, function (arr, extra) {
      var index = extra.index
        , vals = extra.vals;
      return arr.slice(0, index).concat(vals).concat(arr.slice(index));
    });
    adapter.findOne(collection, {_id: _id}, fields, cb);
  });

  store.route('shift', '*.*.*', -1000, function shiftCb (collection, id, relPath, ver, done, next) {
    var fields = { _id: 0 };
    fields[relPath] = 1;
    var _id = maybeCastId.toDb(id);
    adapter.findOne(collection, {_id: _id}, fields, createFindOneCb(collection, _id, relPath, ver, done, function (arr) {
      var copy = arr.slice();
      copy.shift();
      return copy;
    }));
  });

  store.route('remove', '*.*.*', -1000, function removeCb (collection, id, relPath, index, count, ver, done, next) {
    var fields = { _id: 0 };
    fields[relPath] = 1;
    var _id = maybeCastId.toDb(id);
    adapter.findOne(collection, {_id: _id}, fields
      , createFindOneCb(collection, _id, relPath, ver, done, {index: index, count: count}, function (arr, extra) {
          var copy = arr.slice();
          var index = extra.index;
          var count = extra.count;
          copy.splice(index, count);
          return copy;
        })
    );
  });

  store.route('move', '*.*.*', -1000, function moveCb (collection, id, relPath, from, to, count, ver, done, next) {
    var fields = { _id: 0 };
    fields[relPath] = 1;
    var _id = maybeCastId.toDb(id);
    adapter.findOne(collection, {_id: _id}, fields
      , createFindOneCb(collection, _id, relPath, ver, done, {to: to, from: from, count: count}, function (arr, extra) {
          var copy = arr.slice();
          var to = extra.to
            , from = extra.from
            , count = extra.count;
          if (to < 0) to += copy.length;
          var values = arr.splice(from, count);
          var args = [to, 0].concat(values);
          arr.splice.apply(arr, args);
          return arr;
        })
    );
  });
};

var MongoCollection = mongo.Collection;

function Collection (name, db) {
  this.name = name;
  this.db = db;
  this._pending = [];
  this._ready = false;

  var self = this;
  db.collection(name, function (err, collection) {
    if (err) throw err;
    self._ready = true;
    self.collection = collection;
    self.onReady();
  });
}

Collection.prototype.onReady = function onReady () {
  var pending = this._pending
    , currPending, name, args;
  while (currPending = pending.shift()) {
    name = currPending[0];
    args = currPending[1];
    this[name].apply(this, args);
  }
};

var mongoCollectionProto = MongoCollection.prototype;
var collectionProto = Collection.prototype;
for (var name in mongoCollectionProto) {
  collectionProto[name] = (function (name, fn) {
    return function () {
      var collection = this.collection
        , args = arguments;
      if (this._ready) {
        process.nextTick( function () {
          collection[name].apply(collection, args);
        });
      } else {
        this._pending.push([name, args]);
      }
    };
  })(name, mongoCollectionProto[name]);
}
