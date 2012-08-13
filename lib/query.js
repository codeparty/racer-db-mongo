var util = require('./util')
  , fixId = util.fixId

module.exports = createQueryCtor;

function createQueryCtor (racer) {
  var Promise = racer.util.Promise;

  function MongoQuery (queryJson) {
    this._conds = {};
    this._opts = {};
    for (var k in queryJson) {
      if (k === 'type') {
        this._type = queryJson[k];
      } else if (k in this) {
        this[k](queryJson[k]);
      }
    }
  }

  MongoQuery.prototype.from = function from (ns) {
    this._namespace = ns;
    return this;
  };

  MongoQuery.prototype.byKey = function byKey (keyVal) {
    this._conds._id = keyVal;
    return this;
  };

  MongoQuery.prototype.equals = function equals (params) {
    var conds = this._conds;
    for (var path in params) {
      var val = params[path]; // Grab val before potential 'id' -> '_id'
      if (path === 'id') path = '_id';
      conds[path] = val;
    }
    return this;
  };

  var methods = {
      notEquals: '$ne'
    , gt: '$gt'
    , gte: '$gte'
    , lt: '$lt'
    , lte: '$lte'
    , within: '$in'
    , contains: '$all'
    , exists: '$exists'
  };

  for (var k in methods) {
    MongoQuery.prototype[k] = (function (descriptor) {
      return function (params) {
        var conds = this._conds;
        for (var path in params) {
          var val = params[path]; // Grab val before potential 'id' -> '_id'
          if (path === 'id') path = '_id';
          var cond = conds[path] || (conds[path] = {});
          cond[descriptor] = val;
        }
        return this;
      };
    })(methods[k]);
  }

  methods = {
      only: 1
    , except: 0
  };
  for (k in methods) {
    MongoQuery.prototype[k] = (function (flag) {
      // e.g., `only(paths...)`, `except(paths...)`
      return function (params) {
        var opts = this._opts
          , fields = opts.fields || (opts.fields = {});
        for (var path in params) {
          if (path === 'id') path = '_id';
          fields[path] = flag;
        }
        return this;
      };
    })(methods[k]);
  }

  ['skip', 'limit'].forEach( function (method) {
    MongoQuery.prototype[method] = function (val) {
      this._opts[method] = val;
      return this;
    };
  });

  MongoQuery.prototype.sort = function (params) {
    var sort = this._opts.sort = []
      , path, dir;
    for (var i = 0, l = params.length; i < l; i+= 2) {
      path = params[i];
      if (path === 'id') path = '_id';
      dir = params[i+1];
      sort.push([path, dir]);
    }
    return this;
  };

  MongoQuery.prototype.run = function run (mongoAdapter, cb) {
    var promise = (new Promise).on(cb)
      , opts = this._opts;
    if (('limit' in opts) && ! ('skip' in opts)) {
      this.skip(0);
    }
    var self = this;
    var type = this._type;
    var method;
    if (type === 'find') {
      return mongoAdapter.find(this._namespace, this._conds, opts, function (err, found) {
        if (err) return promise.resolve(err);
        found.forEach(fixId);
        promise.resolve(null, found);
      });
    }

    if (type === 'findOne' || type === 'one') {
      return mongoAdapter.findOne(this._namespace, this._conds, opts, function (err, found) {
        if (err) return promise.resolve(err);
        if (found) fixId(found);
        promise.resolve(null, found);
      });
    }

    if (type === 'count') {
      return mongoAdapter.count(this._namespace, this._conds, opts, function (err, count) {
        if (err) return promise.resolve(err);
        promise.resolve(null, count);
      });
    }
    return promise;
  };

  return MongoQuery;
}
