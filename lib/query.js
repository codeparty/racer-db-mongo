module.exports = createQueryCtor;

function createQueryCtor (racer) {
  var Promise = racer.util.Promise
    , LiveQuery = racer.protected.pubSub.LiveQuery;

  function MongoQuery (query) {
    this._conds = {};
    this._opts = {};
    LiveQuery.call(this, query);
  }

  MongoQuery.prototype.from = function from (ns) {
    this._namespace = ns;
    return this;
  };

  MongoQuery.prototype.byKey = function byKey (keyVal) {
    this._conds._id = keyVal;
    return this;
  };

  MongoQuery.prototype.where = function where (property) {
    this._currProp = property;
    return this;
  };

  MongoQuery.prototype.equals = function equals (val) {
    this._conds[this._currProp] = val;
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
  };

  for (var k in methods) {
    MongoQuery.prototype[k] = (function (descriptor) {
      return function (val) {
        var currProp = this._currProp
          , conds = this._conds
          , cond = conds[currProp] || (conds[currProp] = {});
        cond[descriptor] = val;
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
      return function () {
        var opts = this._opts
          , fields = opts.fields || (opts.fields = {});
        for (var i = arguments.length; i--; ) {
          fields[arguments[i]] = flag;
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

  MongoQuery.prototype.sort = function () {
    var sort = this._opts.sort = []
      , path, dir;
    for (var i = 0, l = arguments.length; i < l; i+= 2) {
      path = arguments[i];
      dir = arguments[i+1];
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
    mongoAdapter.find(this._namespace, this._conds, opts, function (err, found) {
      if (found) {
        if (Array.isArray(found)) {
          found.forEach(fixId);
        } else {
          fixId(found);
        }
      }
      promise.resolve(err, found);
    });
    return promise;
  };

  return MongoQuery;
}

function fixId (doc) {
  doc.id = doc._id;
  delete doc._id;
}
