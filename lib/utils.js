
var _ = require('lodash');

var utils = module.exports = {
  escapeId: function(val) {
    if(_.isArray(val)) {
      return _.map(val, utils.escapeId).join(', ');
    }

    return '"' + val.toString().replace(/"/g, '""').replace(/\./g, '"."') + '"';
  },

  escape: function escape(val, stringifyObjects, timeZone) {
    if (val === undefined || val === null) {
      return 'NULL';
    }

    switch (typeof val) {
      case 'boolean': return (val) ? 'true' : 'false';
      case 'number': return val+'';
    }

    if (val instanceof Date) {
      val = utils.dateToString(val, timeZone || 'local');
    }

    if (Buffer.isBuffer(val)) {
      return utils.bufferToString(val);
    }

    if (_.isArray(val)) {
      return utils.arrayToList(val, timeZone);
    }

    if (_.isObject(val)) {
      if (stringifyObjects) {
        val = val.toString();
      } else {
        return utils.objectToValues(val, timeZone);
      }
    }

    val = val.replace(/'/g, "''");
    return "'"+val+"'";
  },

  
  dateToString: function dateToString(date, timeZone) {
    var dt = new Date(date);

    var year, month, day;
    var hour, minute, second, millisecond;

    if (timeZone === 'local') {
      year        = dt.getFullYear();
      month       = dt.getMonth() + 1;
      day         = dt.getDate();
      hour        = dt.getHours();
      minute      = dt.getMinutes();
      second      = dt.getSeconds();
      millisecond = dt.getMilliseconds();

    } else {
      var tz = convertTimezone(timeZone);

      if (tz !== false && tz !== 0) {
        dt.setTime(dt.getTime() + (tz * 60000));
      }

      year        = dt.getUTCFullYear();
      month       = dt.getUTCMonth() + 1;
      day         = dt.getUTCDate();
      hour        = dt.getUTCHours();
      minute      = dt.getUTCMinutes();
      second      = dt.getUTCSeconds();
      millisecond = dt.getUTCMilliseconds();
    }

    // YYYY-MM-DD HH:mm:ss.mmm
    return (
      zeroPad(year, 4) + '-' + zeroPad(month, 2) + '-' + zeroPad(day, 2) + ' ' +
      zeroPad(hour, 2) + ':' + zeroPad(minute, 2) + ':' + zeroPad(second, 2) + '.' +
      zeroPad(millisecond, 3)
    );
  },

  toSqlDate: function toSqlDate(date) {
    return (
      date.getFullYear() + '-' +
      ('00' + (date.getMonth()+1)).slice(-2) + '-' +
      ('00' + date.getDate()).slice(-2) + ' ' +
      ('00' + date.getHours()).slice(-2) + ':' +
      ('00' + date.getMinutes()).slice(-2) + ':' +
      ('00' + date.getSeconds()).slice(-2)
    );
  },

  toSqlDateWithoutTime: function toSqlDateWithoutTime(date) {
    return (
      date.getFullYear() + '-' +
      ('00' + (date.getMonth()+1)).slice(-2) + '-' +
      ('00' + date.getDate()).slice(-2)
    );
  },

  isDate: function isDate(dateStr) {
    if(! _.isString(dateStr)) {
      return false;
    }

    var regex = /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/;
    return ! _.isNull(dateStr.match(regex));
  },

  isTimestamp: function isTimestamp(timestampStr) {
    if(_.isDate(timestampStr)) {
      return true;
    }

    if(! _.isString(timestampStr)) {
      return false;
    }

    var regex = /^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$/;
    return ! _.isNull(timestampStr.match(regex));
  },

  bufferToString: function bufferToString(buffer) {
    return "'" + buffer.toString('hex') + "'";
  },

  arrayToList: function arrayToList(array, timeZone) {
    var sql = [];

    array.forEach(function(val) {
      if (_.isArray(val)) {
        sql.push('(' + utils.arrayToList(val, timeZone) + ')');
      } else {
        sql.push(utils.escape(val, true, timeZone));
      }
    });

    return sql.join(', ');
  },

  objectToValues: function objectToValues(object, timeZone) {
    var sql = [];

    _.forEach(object, function(val, key) {
      if(_.isFunction(val)) {
        return;
      }

      sql.push(utils.escapeId(key) + ' = ' + utils.escape(val, true, timeZone));
    });

    return sql.join(', ');
  },

  /**
   * Prepare values
   *
   * Transform a JS date to SQL date and functions
   * to strings.
   */
  prepareValue: function(value) {
    if(_.isUndefined(value) || value === null) return value;

    // Cast functions to strings
    if (_.isFunction(value)) {
      value = value.toString();
    }

    // Store Arrays and Objects as strings
    if (_.isArray(value) || value.constructor && value.constructor.name === 'Object') {
      try {
        value = JSON.stringify(value);
      } catch (e) {
        // just keep the value and let the db handle an error
        value = value;
      }
    }

    // Cast dates to SQL
    if (_.isDate(value)) {
      value = utils.toSqlDate(value);
    }

    return utils.escape(value);
  },
};

function zeroPad(number, length) {
  return _.padStart(number.toString(), length, '0');
}

function convertTimezone(tz) {
  if (tz === 'Z') {
    return 0;
  }

  var m = tz.match(/([\+\-\s])(\d\d):?(\d\d)?/);
  if(! m) return false;

  var sign = (m[1] == '-' ? -1 : 1);
  var firstDigit = parseInt(m[2], 10);
  var secondDigit = ((m[3] ? parseInt(m[3], 10) : 0) / 60);

  return sign * (firstDigit + secondDigit) * 60;
}
