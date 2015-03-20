var Stream = require('stream'),
    through = require('through'),
    unique = require('unique-stream'),
    sortStream = require('sort-stream2'),
    streamify = require('stream-array'),
    subindex = require('subindex');

module.exports = queryengine;
function queryengine(db) {
  db = subindex(db);
  if (!db.query) {
    db.query = query.bind(null, db);
    db.query.use = use.bind(null, db);
  }

  return db;
}

function query(db) {
  var q = [].slice.call(arguments, 1);
  var options = {};
  if(q.length == 2){
    if(typeof q[1] == 'object') options = q[1];
    q = q[0];
  }
  var sortKeys = Object.keys(options.sort || {});
  var sortAttribute = sortKeys.length > 0 ? sortKeys[0] : null;
  var sortOrder =
    sortAttribute != null && typeof options.sort == 'object' ? options.sort[sortAttribute] : null;
  var requiredStreamSort = sortOrder != null ? true : false;
  var requiredStreamLimit = false;
  var candidates;
  var finalStream = through();

  // if our query has both a sort and limit constraint, we try to use indexes
  // to avoid a full table scan
  if(sortOrder != null && options.limit > -1){
    // if we have a sort + a limit, sorting will be done by indexStreams
    // if(options.limit > -1) requiredStreamSort = false;

    // even if sort order is the default one (asc), we need to use indexStream
    // to order contents regarding the sorted property
    var sortQ = merge({}, q);
    var specialQuery = {}
    specialQuery[sortAttribute] = {$exists: true};
    sortQ = merge(sortQ, specialQuery);
    var opts = {
      limit: options.limit,
      reverse: sortOrder === -1 ? true : false
    };
    var indexStream = db.query.engine.query.call(db, sortQ, opts);
    streamToArray(indexStream, function(array){
      // if the stream contains enough document to meet the required limit
      // we have all we need
      if(array.length == options.limit){
        candidatesIndex = arrayToStream(array);
        candidates = candidatesIndex.pipe(unique(keyfn))
          .pipe(createValueStream.call(null,db))
        requiredStreamSort = false;
        handleCandidates(candidates);
      }
      // else as we can't lookup for documents which don't own a property 
      // through indexes, we have to make a regular query (using indexes if possible)
      // but with no limit options and apply the limit afterwards
      else{
        var queryOptions = {
          limit: -1,
          reverse: false
        }
        requiredStreamLimit = true;
        optimizeQuery(q, queryOptions);
      }
    });
  }
  // else we just have to try to satisfy the query with a classic indexStream
  // or do a full table scan if no stream met the query requirements
  else
    optimizeQuery(q, options);
  // this function will try to satisfy a query using indexes if possible
  // else a full table scan will be performed followed by a second filter on 
  // each candidates to match the query
  function optimizeQuery(query, queryOptions) {
    if(typeof queryOptions.limit != 'number') queryOptions.limit = -1;
    var indexStream = db.query.engine.query.call(db, query, queryOptions);
    if (indexStream !== null && indexStream instanceof Stream) {
      candidates = indexStream
        .pipe(unique(keyfn))
        .pipe(createValueStream.call(null, db))
      handleCandidates(candidates);
    } else {
      tableScan(queryOptions);
    }
  }
  function tableScan(queryOptions){
    // table scan
    var tableScanOptions = {
      limit: queryOptions.limit,
      reverse: false
    }
    candidates = db.createReadStream(tableScanOptions).pipe(through(function (data) {
      if (data.value !== undefined) this.queue(data.value);
    }));
    handleCandidates(candidates);
  }
  // helper to apply a StreamLimiter on the fly if required
  function pipeFinalStream (stream) {
    if(requiredStreamLimit)
      stream.pipe(streamLimiter(options.limit)).pipe(finalStream);
    else
      stream.pipe(finalStream);
  }
  // apply an extra matching filter to all candidates and sort result if required
  function handleCandidates(candidates) {
    var values = candidates.pipe(through(
      function write(data) {
        if (db.query.engine.match.apply(db, [data].concat(q))) {
          this.queue(data);
        }
      },
      function end() {
        this.queue(null);
      }));
    if(requiredStreamSort){
      s = values.pipe(sortStream(function(a, b){
        var aValue = getValueForPath(a, sortAttribute);
        var bValue = getValueForPath(b, sortAttribute);
        if(aValue != undefined && bValue != undefined){
          return (aValue - bValue) * sortOrder;
        }
        else{
          if(aValue == undefined && bValue == undefined) return 0;
          return (aValue != undefined ? 1 : -1) * sortOrder;
        }
      }))
      pipeFinalStream(s);
    }
    else{
      pipeFinalStream(values);
    }
  }
  return finalStream;
}

function use(db, queryEngine) {
  db.query.engine = queryEngine;
}

function keyfn(index) {
  return index.key[index.key.length - 1];
}

function createValueStream(db) {
  var s = new Stream();
  s.readable = true;
  s.writable = true;

  var work = 0;
  var ended = false;

  s.write = function (data) {
    work++;
    db.get(keyfn(data), function (err, value) {
      if (!err) s.emit('data', value);
      if (--work === 0 && ended) s.end();
    });
  };

  s.end = function (data) {
    ended = true;
    if (arguments.length) s.write(data);
    if (work === 0 && s.writable) {
      s.writable = false;
      s.emit('end');
    }
  };

  s.destroy = function () {
    s.writable = false;
  };

  return s;
}

function streamToArray(stream, callback) {
  var buffer = []
  stream.on('data', function(data){ buffer.push(data); })
  .on('end', callback.bind(null,buffer));
}

function arrayToStream(buffer){
  return streamify(buffer);
}

function streamLimiter(limit) {
  i = 0
  return through(function(data){ 
    if(i++ < limit) this.queue(data);
  });
}

function getValueForPath(obj, path){
  var paths = path.split('.'),
    current = obj;
  for(var i = 0; i < paths.length; ++i){
    if(typeof current != 'object' && i < path.length -1) return undefined;
    // keep in mind that undefined == null in js
    if(current[paths[i]] == null)
      return current[paths[i]];
    else
      current = current[paths[i]];
  }
  return current;
}

function merge(orig, extend) {
    for( k in extend){
      orig[k] = extend[k]
    }
    return orig
  }
