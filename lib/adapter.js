/**
 * Module Dependencies
 */

// General dependencies
var _ = require('lodash');
var Promise = require('bluebird');

// Waterline dependencies
var Errors = require('waterline-errors').adapter;
var Cursor = require('waterline-cursor');

// SQL generator - based on waterline-sequel, then hacked for
// derby.  Candidate for refactoring.
var Sequel = require('waterline-sequel-derby');

// JDBC / Derby dependencies
var JDBC = require('jdbc');
var jinst = require('jdbc/lib/jinst');
var Statement = require('jdbc/lib/statement');

// Local dependencies
var initializer = require('../initializer');
var utils = require('./utils');
var sql = require('./sql');

// NOTE: Taken verbatim from node-waterline-mysql
// Hack for development - in future versions, allow
// logger to be injected (see wl2
// or tweet @mikermcneil for status of this feature or
// to help out)
var log = (process.env.LOG_QUERIES === 'true') ? console.log : function () {};

/**
 * Promisification
 */
(function() {
  var toPromisify = [
    'jdbc/lib/callablestatement',
    'jdbc/lib/connection',
    'jdbc/lib/databasemetadata',
    'jdbc/lib/jdbc',
    'jdbc/lib/pool',
    'jdbc/lib/preparedstatement',
    'jdbc/lib/resultset',
    'jdbc/lib/resultsetmetadata',
    'jdbc/lib/statement',
  ];

  toPromisify.forEach(function(file) {
    var proto = require(file).prototype;
    Promise.promisifyAll(proto);
  });
})();


/**
 * sails-derby
 *
 * Most of the methods below are optional.
 *
 * If you don't need / can't get to every method, just implement
 * what you have time for.  The other methods will only fail if
 * you try to call them!
 *
 * For many adapters, this file is all you need.  For very complex adapters,
 * you may need more flexiblity.  In any case, it's probably a good idea to
 * start with one file and refactor only if necessary.  If you do go that
 * route, it's conventional in Node to create a `./lib` directory for your
 * private submodules and load them at the top of the file with other
 * dependencies.  e.g. var update = `require('./lib/update')`;
 */
module.exports = (function () {

  // Maintains a reference to each connection that gets registered
  // with this adapter.
  var connections = {};

  // Sql options used by Sequel SQL builder
  var sqlOptions = {
    parameterized: false,
    caseSensitive: true,
    escapeCharacter: '"',
    casting: true,
    canReturnValues: false,
    escapeInserts: true,
    declareDeleteAlias: false,
  };

  var adapter = {

    // Set to true if this adapter supports (or requires) things like data
    // types, validations, keys, etc. If true, the schema for models using
    // this adapter will be automatically synced when the server starts.
    // Not terribly relevant if your data store is not SQL/schemaful.
    //
    // If setting syncable, you should consider the migrate option, which
    // allows you to set how the sync will be performed.  It can be
    // overridden globally in an app (config/adapters.js) and on a per-model
    // basis.
    //
    // IMPORTANT:
    // `migrate` is not a production data migration solution!  In production,
    // always use `migrate: safe`
    //
    // drop   => Drop schema and data, then recreate it
    // alter  => Drop/add columns as necessary.
    // safe   => Don't change anything (good for production DBs)
    //
    // NOTE: "This may be changed to true in the future." - Lead Developer,
    // Derby Adapter.
    //  
    syncable: true,


    // Default configuration for connections
    defaults: {
      // JVM options
      jvmOptions: '-Xrs',
      jars: initializer.defaultJars([
        'derby.jar',
        'derbyclient.jar',
        'derbytools.jar',
      ]),

      /// Required connection options
      // url: 'jdbc:derby://localhost:1527/TEST;ssl=basic;user=test;password=test',

      /// Optional connection options
      // drivername: 'my.jdbc.DriverName',
      // minpoolsize: 10,
      // maxpoolsize: 100,
      // user: 'user',
      // password: 'password',
      // properties: {}
    },


    /**
     * Verifies that this is the Derby adapter.
     */
    isDerby: function() {
      return true;
    },


    /**
     *
     * This method runs when a model is initially registered
     * at server-start-time.  This is the only required method.
     *
     * NOTE: Heavily borrows from node-waterline-mysql.
     *
     * @param  {[type]}   connection [description]
     * @param  {[type]}   collections [description]
     * @param  {Function} cb         [description]
     * @return {[type]}              [description]
     */
    registerConnection: function(connection, collections, cb) {
      log('Derby.registerConnection()');

      // Check for and handle errors
      if(!connection.identity) {
        return cb(Errors.IdentityMissing);
      }
      if(connections[connection.identity]) {
        return cb(Errors.IdentityDuplicate);
      }
      if(! connection.url) {
        return cb(new Error(
          'Derby adapter: Invalid configuration: URL is required ' +
          'but not present.'
        ));
      }

      // Build up a schema for this connection that can be used throughout the
      // adapter
      var schema = {};

      _.forEach(_.filter(collections), function(collection, collName) {
        var _schema = (
          collection.waterline &&
          collection.waterline.schema &&
          collection.waterline.schema[collection.identity]
        );

        if(! _schema) return;

        // Set defaults to ensure values are set
        _.defaults(_schema, {
          attributes: {},
          tableName: collName
        });

        // If the connection names aren't the same we don't need it in the schema
        if(!_.includes(collection.connection, connection.identity)) {
          return;
        }

        // Store the schema
        schema[_schema.tableName] = _schema;
      });

      // Add jvm options and jar files
      if(! initializer.isInitialized()) {
        initializer.initialize(connection.jvmOptions, connection.jars);
      }

      // Create connection object
      var activeConnection = connections[connection.identity] = {
        config: connection,
        collections: collections,
        connection: {},
        schema: schema
      };

      // Setup connection
      var config = _.omit(connection, ['jvmOptions', 'jars']);

      // Create actual JDBC connection
      activeConnection.connection.pool = new JDBC(config);
      activeConnection.connection.pool.initializeAsync().then(function() {
        cb();
        return null;
      }).catch(function(err) {
        return cb(err);
        return null;
      });
    },


    /**
     * Fired when a model is unregistered, typically when the server
     * is killed. Useful for tearing-down remaining open connections,
     * etc.
     *
     * @param  {Function} cb [description]
     * @return {[type]}      [description]
     */
    // Teardown a Connection
    teardown: function (connection, cb) {
      log('Derby.teardown()');

      if (typeof connection == 'function') {
        cb = connection;
        connection = null;
      }
      if (!connection) {
        connections = {};
        return cb();
      }
      if(!connections[connection]) return cb();
      delete connections[connection];
      cb();
    },

    /**
     * Direct access to the JDBC connection pool for *real* manual control.
     */
    getConnection: function(connection, cb) {
      log('Derby.getConnection');

      var db = getDbConnection(connection);
      return getPool(db);
    },

    /**
     * Direct access to make a query (READ).
     */
    query: function(connection, table, query, values, cb) {
      log('Derby.query: ', table, query, values);

      // Process parameters
      // - Values is optional; will use a prepared statement if given
      if(_.isFunction(values)) {
        cb = values;
        values = null;
      }

      // Get database connection
      var db = getDbConnection(connection);

      // Detect and handle errors
      if(! db.collections[table]) {
        var err = new Error('Derby adapter: Unknown table: ' + table)
        cb && cb(err);
        return Promise.reject(err);
      }

      // Get schema
      var schema = db.schema;

      // Build options
      var options = {};

      if(values) {
        options.values = values;
      }

      return execQuery(db, query, options).then(function(results) {
        results = sql.normalizeResults(schema[table], results);
        cb && cb(null, results);
        return results;

      }).catch(function(err) {
        cb && cb(err);
        throw err;
      });
    },

    /**
     * Direct access to make a query (UPDATE).
     */
    queryUpdate: function(connection, table, query, values, cb) {
      log('Derby.queryUpdate: ', table, query, values);

      // Process parameters
      // - Values is optional; will use a prepared statement if given
      if(_.isFunction(values)) {
        cb = values;
        values = null;
      }

      // Get database connection
      var db = getDbConnection(connection);

      // Detect and handle errors
      if(! db.collections[table]) {
        return cb(new Error('Derby adapter: Unknown table: ' + table));
      }

      // Get schema
      var schema = db.schema;

      // Build options
      var options = {};

      if(values) {
        options.values = values;
      }

      return execUpdate(db, query, options).then(function(results) {
        cb();
        return results;
      }).catch(function(err) {
        cb(err);
        throw err;
      });
    },

    // Convenience date methods
    toDate: utils.toSqlDateWithoutTime,
    toTimestamp: utils.toSqlDate,

    join: function (connection, table, options, cb) {
      log('Derby.join: ', table, options);

      Cursor({
        instructions: options,
        nativeJoins: true,

        /**
         * Find some records directly (using only this adapter)
         * from the specified collection.
         *
         * @param  {String}   table    - Name of the table
         * @param  {Object}   criteria
         * @param  {Function} _cb
         */
        $file: function(table, criteria, cb) {
          log('Derby.join.$file', table, criteria);
          return adapter.find(connection, table, criteria, cb);
        },

        /**
         * Look up the name of the primary key field
         * for the collection with the specified identity.
         *
         * @param  {String}   table - Name of the table
         * @return {String}
         */
        $getPK: function (table) {
          log('Derby.join.$getPK', table);

          if (!table) return;
          return _getPK(connection, table);
        },

        /**
         * Given a strategy type, build up and execute a SQL query for it.
         *
         * @param {}
         */

        $populateBuffers: function populateBuffers(options, next) {
          log('Derby.join.$populateBuffers', options);

          var buffers = options.buffers;
          var instructions = options.instructions;

          // Get database connection
          var db = getDbConnection(connection);

          // Detect and handle errors
          if(! db.collections[table]) {
            return cb(new Error('Derby adapter: Unknown table: ' + table));
          }

          // Prepare for processing
          var parentRecords = [];
          var cachedChildren = {};

          // Build queries / query
          var sequel = new Sequel(db.schema, sqlOptions);
          var queries;

          try {
            queries = sequel.find(table, instructions).query;
          } catch(e) {
            return next(e);
          }

          var query = queries.shift();

          log('Derby.join.$populateBuffers.SQL:', query);

          // Query db
          execQuery(db, query).then(function(results) {
            // Build parentRecords
            parentRecords = sql.normalizeResults(db.schema[table], results);

            // Build cachedChildren - Pull out any aliased child records that
            // have come from a hasFK association
            _.forEach(parentRecords, function(parent) {
              var cache = {};

              _.forEach(_.keys(parent), function(key) {
                // Check if we can split this on our special alias identifier
                // '___' and if so put the result in the cache
                var split = key.split('___');
                if(split.length < 2) return;

                if(_.isUndefined(cache[split[0]])) {
                  cache[split[0]] = {};
                }
                cache[split[0]][split[1]] = parent[key];
                delete parent[key];
              });

              // Combine the local cache into the cachedChildren
              _.forEach(cache, function(value, key) {
                if(_.isUndefined(cachedChildren[key])) {
                  cachedChildren[key] = [];
                }
                cachedChildren[key] = cachedChildren[key].concat(value);
              });
            });

            buffers.parents = parentRecords;

            // Build child buffers - For each instruction, loop through the
            // parent records and build up a buffer for the record
            _.forEach(instructions.instructions, function(population, popKey) {
              var popInstruct = _.first(population.instructions);
              var pk = _getPK(connection, popInstruct.parent);
              var alias = (
                population.strategy.strategy === 1 ?
                popInstruct.parentKey :
                popInstruct.alias
              );

              _.forEach(parentRecords, function(parent) {
                var buffer = {
                  attrName: popKey,
                  parentPK: parent[pk],
                  pkAttr: pk,
                  keyName: alias,
                };

                var records = [];
                var recordsMap = {};

                // Check for cached parent records
                if(! _.isUndefined(cachedChildren[alias])) {
                  _.forEach(cachedChildren[alias], function(cachedChild) {
                    var childVal = popInstruct.childKey;
                    var parentVal = popInstruct.parentKey;

                    if(cachedChild[childVal] !== parent[parentVal]) {
                      return; // Lodash continue
                    }

                    // If null value for the parentVal, ignore it
                    if(_.isNull(parent[parentVal])) {
                      return; // Lodash continue
                    }

                    // If the same record is already there, ignore it
                    if(recordsMap[cachedChild[childVal]]) {
                      return; // Lodash continue
                    }

                    records.push(cachedChild);
                    recordsMap[cachedChild[childVal]] = true;
                  });
                }

                if(records.length > 0) {
                  buffer.records = records;
                }

                buffers.add(buffer);
              });
            });

            // Process children
            var preppedQueries = [];
            _.forEach(queries, function(query) {
              var qs = '';
              var pk;

              if(! _.isArray(query.instructions)) {
                pk = _getPK(connection, query.instructions.parent);
              } else if(query.instructions.length > 1) {
                pk = _getPK(connection, _.first(query.instructions).parent);
              }

              var unions = [];
              _.forEach(parentRecords, function(parent) {
                if(_.isNumber(parent[pk])) {
                  unions.push(query.qs.replace('^?^', parent[pk]));
                  return; // Lodash continue
                }
                unions.push(query.qs.replace('^?^', "'" + parent[pk] + "'"));
              });
              qs += unions.join(' UNION ');

              if(parentRecords.length > 1) {
                var toSort = [];

                if(! _.isArray(query.instructions)) {
                  toSort = query.instructions.criteria.sort;
                } else if(query.instructions.length === 2) {
                  toSort = query.instructions[1].criteria.sort;
                }

                var addedOrder = false;
                _.forEach(toSort, function(sortVal, sortKey) {
                  if(! sortKey.match(/^[0-9a-zA-Z$_]+$/)) return;
                  if(! addedOrder) {
                    addedOrder = true;
                    qs += ' ORDER BY ';
                  }

                  var direction = sortVal === 1 ? 'ASC' : 'DESC';
                  qs += utils.escapeId(sortKey) + ' ' + direction;
                });
              }

              log('Derby.processChildren: ', qs);

              preppedQueries.push({
                qs: qs,
                query: query
              });
            });

            function runQuery(preppedQuery) {
              log('Derby.join.$populateBuffers.runQuery', preppedQuery);

              var query = preppedQuery.qs;
              var instructs = preppedQuery.query.instructions;

              return execQuery(db, query).then(function(results) {
                var groupedRecords = {};

                _.forEach(results, function(row) {
                  if(! _.isArray(instructs)) {
                    if(_.isUndefined(groupedRecords[row[instructs.childKey]])) {
                      groupedRecords[row[instructs.childKey]] = [];
                    }

                    groupedRecords[row[instructs.childKey]].push(row);

                    return; // Lodash continue
                  }

                  // Grab the special "foreign key" we attach and make sure to
                  // remove it
                  var fk = '___' + _.first(instructs).childKey;

                  if(_.isUndefined(groupedRecords[row[fk]])) {
                    groupedRecords[row[fk]] = [];
                  }

                  var data = _.cloneDeep(row);
                  delete data[fk];
                  groupedRecords[row[fk]].push(data);
                });

                _.forEach(buffers.store, function(buffer) {
                  if(buffer.attrName !== preppedQuery.query.attrName) return;
                  var records = groupedRecords[buffer.belongsToPKValue];
                  if(! records) return;
                  if(! buffer.records) buffer.records = [];
                  buffer.records = buffer.records.concat(records);
                });
              });
            }

            return Promise.resolve(preppedQueries).each(runQuery);

          }).then(function() {
            next();
            return null;

          }).catch(function(err) {
            return next(err);
          });
        }

      }, cb);
    },

    // Count one model from the collection
    // using where, limit, skip, and order
    // In where: handle `or`, `and`, and `like` queries
    count: function(connection, table, options, cb) {
      log('Derby.count: ', table, options);

      // Check if this is an aggregate query and that there is something to return
      // NOTE: This is taken from sails-mysql under the (hopefully reasonable)
      // assumption that the same rules apply.
      if(options.groupBy || options.sum || options.average || options.min || options.max) {
        if(!options.sum && !options.average && !options.min && !options.max) {
          return cb(Errors.InvalidGroupBy);
        }
      }

      // Get database connection
      var db = getDbConnection(connection);

      // Detect and handle errors
      if(! db.collections[table]) {
        return cb(new Error('Derby adapter: Unknown table: ' + table));
      }

      // Get schema
      var schema = db.schema;

      // Create SQL builder (Sequel)
      var sequel = new Sequel(schema, sqlOptions);

      // Build query
      var query;
      try {
        query = _.first(sequel.count(table, options).query);
      } catch(err) {
        return cb(err);
      }

      return execQuery(db, query).then(function(results) {
        var ct = _.first(results).count;
        cb(null, ct);
        return ct;

      }).catch(function(err) {
        cb(err);
        throw err;
      });
    },


    // Return attributes
    describe: function (connection, table, cb) {
      log('Derby.describe: ', table);

      // Get database connection
      var db = getDbConnection(connection);

      // Detect and handle errors
      if(! db.collections[table]) {
        return cb(new Error('Derby adapter: Unknown table: ' + table));
      }

      // Get connection pool
      var pool = getPool(db);

      // Reserve connection, query database
      var scope = {};
      return pool.reserveAsync().then(function(connObj) {
        scope.connObj = connObj;
        var conn = connObj && connObj.conn;

        if(! conn) {
          throw new Error('Error: No connection is available.');
        }

        return conn.getMetaDataAsync();

      }).then(function(metadata) {
        var promises = [
          metadata.getColumnsAsync(null, null, table, null),
          metadata.getIndexInfoAsync(null, null, table, false, false),
          metadata.getPrimaryKeysAsync(null, null, table),
        ];

        return Promise.all(promises);

      }).then(function(fulfilled) {
        var promises = [
          fulfilled[0].toObjArrayAsync(),
          fulfilled[1].toObjArrayAsync(),
          fulfilled[2].toObjArrayAsync(),
        ];

        return Promise.all(promises);

      }).then(function(fulfilled) {
        var columns = fulfilled[0];
        var indices = fulfilled[1];
        var primaryKeys = fulfilled[2];

        // If there are no columns, treat like there is no table and return null
        if(columns.length < 1) {
          return pool.releaseAsync(scope.connObj).then(function() {
            delete scope.connObj;
            cb();
            return null;
          });
        }

        // Convert into standard waterline schema
        scope.normalizedSchema = sql.normalizeSchema(
          columns, indices, primaryKeys
        );

        // Release connection
        return pool.releaseAsync(scope.connObj).then(function() {
          delete scope.connObj;

          // Set internal schema mapping
          db.collections[table].schema = scope.normalizedSchema;

          // Return data to callback
          cb(null, scope.normalizedSchema);
          return scope.normalizedSchema;
        });

      }).catch(function(err) {
        if(scope.connObj) {
          return pool.releaseAsync(scope.connObj).then(function() {
            cb(err);
            throw err;
          });
        }

        cb(err);
        throw err;
      });
    },

    /**
     *
     * REQUIRED method if integrating with a schemaful
     * (SQL-ish) database.
     *
     */
    define: function (connection, table, definition, cb) {
      log('Derby.define: ', table, definition);

      // Get database connection
      var db = getDbConnection(connection);

      // Detect and handle errors
      if(! db.collections[table]) {
        return cb(new Error('Derby adapter: Unknown table: ' + table));
      }

      // Build schema
      var schema = sql.schema(table, definition);

      // Build query
      var query = (
        'CREATE TABLE ' + utils.escapeId(table) + ' (' + schema + ')'
      );

      // Execute query
      execUpdate(db, query).then(function(results) {
        adapter.describe(connection, table, function(err, result) {
          if(err) {
            throw err;
          }
          cb(null, result);
          return result;
        });

      }).catch(function(err) {
        cb(err);
        throw err;
      });
    },

    /**
     *
     * REQUIRED method if integrating with a schemaful
     * (SQL-ish) database.
     *
     */
    drop: function (connection, table, relations, cb) {
      log('Derby.drop: ', table, relations);

      if(_.isFunction(relations)) {
        cb = relations;
        relations = [];
      }

      // Get database connection
      var db = getDbConnection(connection);

      // Detect and handle errors
      if(! db.collections[table]) {
        return cb(new Error('Derby adapter: Unknown table: ' + table));
      }

      // Build query
      var query = 'DROP TABLE ' + utils.escapeId(table);

      // Execute query
      execUpdate(db, query).then(function() {
        cb();
        return null;

      }).catch(function(err) {
        // Ignore error: "ERROR 42Y55: 'DROP TABLE' cannot be performed on ... 
        //                because it does not exist."
        if(err.stack.indexOf('ERROR 42Y55:') !== -1) {
          return cb();
        }

        cb(err);
        throw err;
      });

      // TODO Not sure how to handle relations, ignoring for now
    },

    /**
     *
     * REQUIRED method if users expect to call Model.find(), Model.findOne(),
     * or related.
     *
     * You should implement this method to respond with an array of instances.
     * Waterline core will take care of supporting all the other different
     * find methods/usages.
     *
     */
    find: function (connection, table, options, cb) {
      log('Derby.find: ', table, options);

      // Check if this is an aggregate query and that there is something to return
      // NOTE: This is taken from sails-mysql under the (hopefully reasonable)
      // assumption that the same rules apply.
      if(options.groupBy || options.sum || options.average || options.min || options.max) {
        if(!options.sum && !options.average && !options.min && !options.max) {
          return cb(Errors.InvalidGroupBy);
        }
      }

      // Get database connection
      var db = getDbConnection(connection);

      // Detect and handle errors
      if(! db.collections[table]) {
        return cb(new Error('Derby adapter: Unknown table: ' + table));
      }

      // Get schema
      var schema = db.schema;

      // Create SQL builder (Sequel)
      var sequel = new Sequel(schema, sqlOptions);

      // Build query
      var query;
      try {
        query = _.first(sequel.find(table, options).query);
      } catch(err) {
        return cb(err);
      }

      // Execute query
      return execQuery(db, query).then(function(results) {
        var normResults = sql.normalizeResults(schema[table], results);
        cb(null, normResults);
        return normResults;

      }).catch(function(err) {
        cb(err);
        throw err;
      });
    },

    create: function (connection, table, values, cb) {
      log('Derby.create: ', table, values);

      // Get database connection
      var db = getDbConnection(connection);

      // Detect and handle errors
      if(! db.collections[table]) {
        var error = new Error('Derby adapter: Unknown table: ' + table);
        return cb(error);
      }

      // Prepare / sanitize values into new fields list
      var fields = [];
      _.forEach(values, function(value, field) {
        fields[field] = utils.prepareValue(value);
      });

      // Get the schema
      var schema = db.schema;

      // Create SQL builder (Sequel)
      var sequel = new Sequel(schema, sqlOptions);

      // Build query
      var query;
      try {
        query = sequel.create(table, fields).query;
      } catch(err) {
        return cb(err);
      }

      log('Derby.SQL: ', query, ' (UPDATE)');

      // Execute query
      var scope = {};
      var pool = getPool(db);
      pool.reserveAsync().then(function(connObj) {
        scope.connObj = connObj;
        var conn = connObj && connObj.conn;

        if(! conn) {
          throw new Error('Error: No connection is available.');
        }

        return conn.prepareStatementAsync(
          query, Statement.RETURN_GENERATED_KEYS
        );

      }).then(function(statement) {
        scope.statement = statement;
        return statement.executeUpdateAsync();

      }).then(function() {
        return scope.statement.getGeneratedKeysAsync();

      }).then(function(resultSet) {
        return resultSet.toObjArrayAsync();

      }).then(function(results) {
        scope.finalValues = sql.mergeGeneratedKeys(schema[table], values, results);

        // Release connection back to pool
        return pool.releaseAsync(scope.connObj);

      }).then(function() {
        delete scope.connObj;

        // Call callback with values
        cb(null, scope.finalValues);
        return scope.finalValues;

      }).catch(function(err) {
        if(scope.connObj) {
          return pool.releaseAsync(scope.connObj).then(function() {
            cb(err);
            throw err;
          });
        }

        cb(err);
        throw err;
      });
    },

    update: function (connection, table, options, values, cb) {
      log('Derby.update: ', table, options, values);

      // Get database connection
      var db = getDbConnection(connection);

      // Detect and handle errors
      if(! db.collections[table]) {
        return cb(new Error('Derby adapter: Unknown table: ' + table));
      }

      // Get collection object
      var collection = db.collections[table];

      // Prepare / sanitize values into new fields list
      var fields = [];
      _.forEach(values, function(value, field) {
        fields[field] = utils.prepareValue(value);
      });

      // Get the schema
      var schema = db.schema;

      // Create SQL builder (Sequel)
      var sequel = new Sequel(schema, sqlOptions);

      // Build query
      var query;
      try {
        query = _.first(sequel.find(table, _.cloneDeep(options)).query);
      } catch(err) {
        return cb(err);
      }

      // Execute query
      execQuery(db, query).then(function(results) {
        // If nothing was found, nothing will be updated; return early
        if(! results.length) {
          return cb(null, []);
        }

        // Find primary key
        var pk = 'id';
        _.forEach(collection.definition, function(fieldDef, fieldName) {
          if(! fieldDef.hasOwnProperty('primaryKey')) return;

          pk = fieldName;

          // Lodash break
          return false;
        });

        // Get ids that will be affected from results
        var ids = [];
        _.forEach(results, function(result) {
          ids.push(result[pk]);
        });

        // Prepare field values
        var fields = [];
        _.forEach(values, function(value, idx) {
          fields[idx] = utils.prepareValue(value);
        });

        // Build update query
        var query = sequel.update(table, options, fields).query;

        // Execute query
        return execUpdate(db, query).then(function(results) {
          // Prepare to build a query to get rows that were updated
          var criteria = { where: {} };
          if(ids.length === 1) {
            criteria.limit = 1;
            criteria.where[pk] = _.first(ids);
          } else {
            criteria.where[pk] = ids;
          }

          // Build query
          var query = _.first(sequel.find(table, criteria).query);

          // Execute query
          return execQuery(db, query);

        }).then(function(results) {
          cb(null, sql.normalizeResults(schema[table], results));
          return results;
        });

      }).catch(function(err) {
        cb(err);
        throw err;
      });
    },

    destroy: function (connection, table, options, cb) {
      log('Derby.destroy: ', table, options);

      // Get database connection
      var db = getDbConnection(connection);

      // Detect and handle errors
      if(! db.collections[table]) {
        return cb(new Error('Derby adapter: Unknown table: ' + table));
      }

      // Get collection object
      var collection = db.collections[table];

      // Get the schema
      var schema = db.schema;

      // Create SQL builder (Sequel)
      var sequel = new Sequel(schema, sqlOptions);

      // Build query
      var query;
      try {
        query = sequel.destroy(table, options).query;
      } catch(err) {
        return cb(err);
      }
      
      // Get records about to be destroyed
      adapter.find(connection, table, options, function(err, results) {
        // Run query to destroy records
        execUpdate(db, query).then(function() {
          cb(null, results);
          return results;

        }).catch(function(err) {
          cb(err);
          throw err;
        });
      });
    }
  };


  // Expose adapter definition
  return adapter;


  ///
  // Utilities
  ///

  function getDbConnection(connection) {
    var activeConnection = connections[connection];

    if(! activeConnection) {
      adapter.emit('error', Errors.InvalidConnection);
    }

    return activeConnection;
  }

  function getPool(db) {
    var pool = (
      db.connection &&
      db.connection.pool
    );

    if(! pool) {
      adapter.emit('error', Errors.InvalidConnection);
    }

    return pool;
  }

  function execQuery(db, query, options) {
    options || (options = {});
    log('Derby.execQuery: ', query, options.isUpdate ? ' (UPDATE)' : '');

    var pool = getPool(db);

    var scope = {};
    return pool.reserveAsync().then(function(connObj) {
      scope.connObj = connObj;
      var conn = connObj && connObj.conn;

      if(! conn) {
        throw new Error('Error: No connection is available.');
      }

      if(options.values) {
        return conn.prepareStatementAsync(query);
      }

      return conn.createStatementAsync();

    }).then(function(statement) {
      if(options.values) {
        var promises = [];

        _.forEach(options.values, function(val, idx) {
          if(_.isInteger(val)) {
            promises.push(statement.setIntAsync(idx + 1, val));
          } else if(_.isString(val)) {
            promises.push(statement.setStringAsync(idx + 1, val));
          } else if(utils.isDate(val)) {
            promises.push(statement.setDateAsync(idx + 1, val));
          } else if(utils.isTimestamp(val)) {
            if(_.isDate(val)) {
              val = utils.toSqlDate(val);
            }
            promises.push(statement.setTimestampAsync(idx + 1, val));
          }
        });

        return Promise.all(promises).then(function() {
          if(options.isUpdate) {
            return statement.executeUpdateAsync();
          }

          return statement.executeQueryAsync();
        });
      }

      if(options.isUpdate) {
        return statement.executeUpdateAsync(query);
      }

      return statement.executeQueryAsync(query);

    }).then(function(resultSet) {
      if(! options.isUpdate && resultSet && resultSet.toObjArray) {
        return resultSet.toObjArrayAsync();
      }

      // Return results
      return resultSet;

    }).then(function(results) {
      scope.results = results;

      // Release connection back to pool
      return pool.releaseAsync(scope.connObj);

    }).then(function() {
      delete scope.connObj;
      return scope.results;

    }).catch(function(err) {
      if(scope.connObj) {
        return pool.releaseAsync(scope.connObj).then(function() {
          throw err;
        });
      }

      throw err;
    });
  }

  function execUpdate(db, query, options) {
    options || (options = {});
    options.isUpdate = true;
    return execQuery(db, query, options);
  }

  /**
   * Lookup the primary key for the given collection
   * @param  {[type]} table - Name of the table (collection)
   * @return {[type]}
   * @api private
   */
  function _getPK (connection, table) {
    var tableDef;
    try {
      tableDef = connections[connection].collections[table].definition;

      return _.find(Object.keys(tableDef), function _findPK (key) {
        var attrDef = tableDef[key];
        if( attrDef && attrDef.primaryKey ) return key;
        else return false;
      }) || 'id';
    }
    catch (e) {
      throw new Error(
        'Unable to determine primary key for collection `'+table+'` because '+
        'an error was encountered acquiring the collection definition:\n'+ 
        require('util').inspect(e,false,null)
      );
    }
  }

})();

