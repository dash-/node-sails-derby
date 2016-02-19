/**
 * Module Dependencies
 */

// General dependencies
var _ = require('lodash');
var Promise = require('bluebird');

// Waterline dependencies
var Errors = require('waterline-errors').adapter;

// SQL generator - based on waterline-sequel, then hacked for
// derby.  Candidate for refactoring.
var Sequel = require('../sequel-derby');

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
(() => {
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

  toPromisify.forEach(file => {
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
    casting: false,
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

      _.forEach(_.filter(collections), (collection, collName) => {
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
      activeConnection.connection.pool.initializeAsync().then(() => {
        cb();
      }).catch(err => {
        return cb(err);
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
      return pool.reserveAsync().then(connObj => {
        scope.connObj = connObj;
        var conn = connObj && connObj.conn;

        if(! conn) {
          throw new Error('Error: No connection is available.');
        }

        return conn.getMetaDataAsync();

      }).then(metadata => {
        var promises = [
          metadata.getColumnsAsync(null, null, table, null),
          metadata.getIndexInfoAsync(null, null, table, false, false),
          metadata.getPrimaryKeysAsync(null, null, table),
        ];

        return Promise.all(promises);

      }).then(fulfilled => {
        var promises = [
          fulfilled[0].toObjArrayAsync(),
          fulfilled[1].toObjArrayAsync(),
          fulfilled[2].toObjArrayAsync(),
        ];

        return Promise.all(promises);

      }).then(fulfilled => {
        var columns = fulfilled[0];
        var indices = fulfilled[1];
        var primaryKeys = fulfilled[2];

        // If there are no columns, treat like there is no table and return null
        if(columns.length < 1) {
          return pool.releaseAsync(scope.connObj).then(() => {
            delete scope.connObj;
            cb();
          });
        }

        // Convert into standard waterline schema
        scope.normalizedSchema = sql.normalizeSchema(
          columns, indices, primaryKeys
        );

        // Release connection
        return pool.releaseAsync(scope.connObj).then(() => {
          delete scope.connObj;

          // Set internal schema mapping
          db.collections[table].schema = scope.normalizedSchema;

          // Return data to callback
          cb(null, scope.normalizedSchema);
        });

      }).catch(err => {
        if(scope.connObj) {
          return pool.releaseAsync(scope.connObj).then(() => {
            cb(err);
          });
        }

        return cb(err);
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
      execUpdate(db, query).then(results => {
        adapter.describe(connection, table, (err, result) => {
          if(err) {
            throw err;
          }
          cb(null, result);
        });

      }).catch(err => {
        return cb(err);
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
      execUpdate(db, query).then(() => {
        cb();

      }).catch(err => {
        // Ignore error: "ERROR 42Y55: 'DROP TABLE' cannot be performed on ... 
        //                because it does not exist."
        if(err.stack.indexOf('ERROR 42Y55:') !== -1) {
          return cb();
        }

        return cb(err);
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
      execQuery(db, query).then(results => {
        cb(null, sql.normalizeResults(schema[table], results));

      }).catch(err => {
        return cb(err);
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
      _.forEach(values, (value, field) => {
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

      log('Derby.query: ', query, ' (UPDATE)');

      // Execute query
      var scope = {};
      var pool = getPool(db);
      pool.reserveAsync().then(connObj => {
        scope.connObj = connObj;
        var conn = connObj && connObj.conn;

        if(! conn) {
          throw new Error('Error: No connection is available.');
        }

        return conn.prepareStatementAsync(
          query, Statement.RETURN_GENERATED_KEYS
        );

      }).then(statement => {
        scope.statement = statement;
        return statement.executeUpdateAsync();

      }).then(() => {
        return scope.statement.getGeneratedKeysAsync();

      }).then(resultSet => {
        return resultSet.toObjArrayAsync();

      }).then(results => {
        scope.finalValues = sql.mergeGeneratedKeys(schema[table], values, results);

        // Release connection back to pool
        return pool.releaseAsync(scope.connObj);

      }).then(() => {
        delete scope.connObj;

        // Call callback with values
        cb(null, scope.finalValues);

      }).catch(err => {
        if(scope.connObj) {
          return pool.releaseAsync(scope.connObj).then(() => {
            cb(err);
          });
        }

        cb(err);
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
      _.forEach(values, (value, field) => {
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
      execQuery(db, query).then(results => {
        // If nothing was found, nothing will be updated; return early
        if(! results.length) {
          return cb(null, []);
        }

        // Find primary key
        var pk = 'id';
        _.forEach(collection.definition, (fieldDef, fieldName) => {
          if(! fieldDef.hasOwnProperty('primaryKey')) return;

          pk = fieldName;

          // Lodash break
          return false;
        });

        // Get ids that will be affected from results
        var ids = [];
        _.forEach(results, result => {
          ids.push(result[pk]);
        });

        // Prepare field values
        var fields = [];
        _.forEach(values, (value, idx) => {
          fields[idx] = utils.prepareValue(value);
        });

        // Build update query
        var query = sequel.update(table, options, fields).query;

        // Execute query
        return execUpdate(db, query).then(results => {
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

        }).then(results => {
          return cb(null, sql.normalizeResults(schema[table], results));
        });

      }).catch(err => {
        return cb(err);
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
      adapter.find(connection, table, options, (err, results) => {
        // Run query to destroy records
        execUpdate(db, query).then(() => {
          cb(null, results);

        }).catch(err => {
          cb(err);
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
    log('Derby.query: ', query, options.isUpdate ? ' (UPDATE)' : '');

    var pool = getPool(db);

    var scope = {};
    return pool.reserveAsync().then(connObj => {
      scope.connObj = connObj;
      var conn = connObj && connObj.conn;

      if(! conn) {
        throw new Error('Error: No connection is available.');
      }

      return conn.createStatementAsync();

    }).then(statement => {
      if(options.isUpdate) {
        return statement.executeUpdateAsync(query);
      }
      return statement.executeQueryAsync(query);

    }).then(resultSet => {
      if(resultSet && resultSet.toObjArray) {
        return resultSet.toObjArrayAsync();
      }

      // Return results
      return resultSet;

    }).then(results => {
      scope.results = results;

      // Release connection back to pool
      return pool.releaseAsync(scope.connObj);

    }).then(() => {
      delete scope.connObj;
      return scope.results;

    }).catch(err => {
      if(scope.connObj) {
        return pool.releaseAsync(scope.connObj).then(() => {
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

})();

