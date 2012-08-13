var EventEmitter = require('events').EventEmitter
  , url = require('url')
  , mongoskin = require('mongoskin')
  , NativeObjectId = mongoskin.BSONPure.ObjectID
  , createQueryCtor = require('./query')
  , util = require('./util')
  , fixId = util.fixId
  , lookup

  , DISCONNECTED  = 1
  , CONNECTING    = 2
  , CONNECTED     = 3
  , DISCONNECTING = 4;

exports = module.exports = plugin;

function plugin (racer) {
  lookup = racer.util.path.lookup
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
  this.options = options;

  // TODO Make version scale beyond 1 db
  //      by sharding and with a vector
  //      clock with each member the
  //      version of a shard
  // TODO Initialize the version properly upon web server restart
  //      so it's synced with the STM server (i.e., Redis) version
  this.version = undefined;
}

DbMongo.prototype.__proto__ = EventEmitter.prototype;

DbMongo.prototype.connect = function connect (options, callback) {
  if (typeof options === 'function') {
    callback = options;
    options = null;
  }
  options = options || this.options || {};

  // TODO: Review the options parsing here
  var self = this
    , uri = options.uri || options.host + ':' + options.port + '/' + options.database + '?' +
        'auto_reconnect=' + ('auto_reconnect' in options ? options.auto_reconnect : true)

  if (callback) this.once('open', callback);
  this.driver = mongoskin.db(uri, options);
  this.driver.open(function(err, db) {
    if (err) console.error(err);
    self.emit('open', err, db);
  });
}

DbMongo.prototype.disconnect = function disconnect (callback) {
  this.driver.close(callback);
};

DbMongo.prototype.flush = function flush (cb) {
  var self = this;
  this.driver.db.dropDatabase( function dropDatabaseCb (err, done) {
    if (err && err.message === 'no open connections') {
      self.on('open', function() {
        self.flush(cb);
      });
      return;
    }
    self.version = 0;
    cb(err, done);
  });
};

// Mutator methods called via CustomDataSource.prototype.applyOps
DbMongo.prototype.update = function update (collection, conds, op, opts, cb) {
  this.driver.collection(collection).update(conds, op, opts, cb);
};

DbMongo.prototype.findAndModify = function findAndModify (collection) {
  var args = Array.prototype.slice.call(arguments, 1);
  var coll = this.driver.collection(collection);
  coll.findAndModify.apply(coll, args);
}

DbMongo.prototype.insert = function insert (collection, json, opts, cb) {
  // TODO Leverage pkey flag; it may not be _id
  var toInsert = Object.create(json);
  toInsert._id || (toInsert._id = new NativeObjectId);
  var coll = this.driver.collection(collection);
  coll.insert(toInsert, opts, function insertCb (err) {
    if (err) return cb(err);
    cb(null, {_id: toInsert._id});
  });
};

DbMongo.prototype.remove = function remove (collection, conds, cb) {
  this.driver.collection(collection).remove(conds, cb);
};

DbMongo.prototype.findOne = function findOne (collection) {
  var args = Array.prototype.slice.call(arguments, 1);
  var coll = this.driver.collection(collection);
  coll.findOne.apply(coll, args);
};

DbMongo.prototype.find = function find (collection, conds, opts, cb) {
  this.driver.collection(collection).find(conds, opts, function findCb (err, cursor) {
    if (err) return cb(err);
    cursor.toArray( function toArrayCb (err, docs) {
      if (err) return cb(err);
      return cb(null, docs);
    });
  });
};

DbMongo.prototype.count = function count (collection, conds, opts, cb) {
  this.driver.collection(collection).count(conds, opts, function findCb (err, count) {
    if (err) return cb(err);
    return cb(null, count);
  });
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
      fixId(doc);
      done(null, doc, adapter.version);
    });
  });

  store.route('get', '*', -1000, function (collection, done, next) {
    adapter.find(collection, {}, {}, function findCb (err, docList) {
      if (err) return done(err);
      var docs = {}, doc;
      for (var i = docList.length; i--; ) {
        doc = docList[i];
        fixId(doc);
        docs[doc.id] = doc;
      }
      return done(null, docs, adapter.version);
    });
  });

  function createCb (ver, done) {
    return function findAndModifyCb (err, origDoc) {
      if (err) return done(err);
      adapter.setVersion(ver);
      if (origDoc && origDoc._id) {
        origDoc.id = maybeCastId.fromDb(origDoc._id);
        delete origDoc._id;
      }
      done(null, origDoc);
    };
  }

  function setCb (collection, _id, relPath, val, ver, done, next) {
    if (relPath === 'id') relPath = '_id';
    var setTo = {};
    setTo[relPath] = val;
    var op = { $set: setTo };
    // TODO Don't let sessions leak into this abstraction
    if (collection !== 'sessions') {
      _id = maybeCastId.toDb(_id);
    }

    // Patch-up implicit creation of arrays in a path, since Mongo
    // will create an object if not already an array
    var cb = /\.[0-9]+(?=\.|$)/.test(relPath) ?
      function (err, origDoc) {
        if (err) return done(err);
        var re = /\.[0-9]+(?=\.|$)/g
          , root;
        while (match = re.exec(relPath)) {
          root = relPath.slice(0, match.index);
          if (lookup(root, origDoc) == null) {
            setCb(collection, _id, root, [], ver, function() {
              setCb(collection, _id, relPath, val, ver, done, next);
            });
            return;
          }
        }
        createCb(ver, done)(err, origDoc);
      } : createCb(ver, done);

    adapter.findAndModify(collection
      , {_id: _id}     // Conditions
      , []             // Empty sort
      , op             // Modification
      , {upsert: true} // If not found, insert the object represented by op
      , cb
    );
  }
  store.route('set', '*.*.*', -1000, setCb);

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
