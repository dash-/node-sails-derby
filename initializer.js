
///
// Dependencies
///

var path = require('path');
var _ = require('lodash');
var jinst = require('jdbc/lib/jinst');


///
// Local variables
///

module.exports = (() => {
  // Default path for JDBC drivers
  var driversPath = path.join(__dirname, 'node_modules', 'jdbc', 'drivers');

  // Keeps track of whether initialization has been performed
  var isInitialized = false;

  // Object for initializing JDBC / Java Instance
  var init = {
    isInitialized: function() {
      return isInitialized || jinst.isJvmCreated();
    },

    initialize: function(options, jars) {
      if(init.isInitialized()) {
        console.error('Cannot initialize JDBC driver: Already initialized.');
        return;
      }

      // Set java command options
      _.isArray(options) || (options = [options]);
      options.forEach(option => {
        jinst.addOption(option);
      });

      // Set java classpaths
      _.isArray(jars) || (jars = [jars]);
      jinst.setupClasspath(jars);

      // Mark initialization as done
      isInitialized = true;
    },

    /**
     * Sets the default drivers path to the path given.
     */
    setDefaultPath: function(path) {
      driversPath = path;
    },

    /**
     * Returns an array of jars matching given jars, but prepended with
     * the default jar path.
     */
    defaultJars: function(jars) {
      _.isArray(jars) || (jars = [jars]);
      return _.map(jars, init.defaultJar);
    },

    /**
     * Returns the jar given but prepended with the default jar path.
     */
    defaultJar: function(jar) {
      return path.join(driversPath, jar);
    },
  };

  jinst.events.on('initialized', () => {
    isInitialized = true;
  });

  return init;
})();
