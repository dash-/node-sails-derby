![image_squidhome@2x.png](http://i.imgur.com/RIvu9.png)

# waterline-derby

Provides easy access to [Apache Derby](https://db.apache.org/derby/) from Sails.js & Waterline.

This module is a Waterline/Sails adapter, an early implementation of a
rapidly-developing, tool-agnostic data standard.  Its goal is to provide a set
of declarative interfaces, conventions, and best-practices for integrating with
all sorts of data sources.  Not just databases-- external APIs, proprietary web
services, or even hardware.

Strict adherence to an adapter specification enables the (re)use of built-in
generic test suites, standardized documentation, reasonable expectations around
the API for your users, and overall, a more pleasant development experience for
everyone.


### Installation

To install this adapter, run:

```sh
$ npm install --save waterline-derby
```

### Usage

This adapter exposes the following methods:

###### `find()`
###### `create()`
###### `update()`
###### `destroy()`


### Interfaces

This adapter implements the semantic, queryable,  associations and sql interfaces.
For more information, check out the [adapter interface reference](https://github.com/balderdashy/sails-docs/blob/master/contributing/adapter-specification.md)
in the Sails docs.


### Development

Set up your connection in `config/connections.js` by adding the following section
and customizing it to your needs:

```javascript
  someDerbyServer: {
    adapter: 'waterline-derby',
    url: 'jdbc:derby://localhost:1527/SOMEDB;ssl=basic;user=ME;password=MINE',
    // minpoolsize: 10,
    // maxpoolsize: 100,
    // user: 'user',
    // password: 'password',
    // properties: {},
    // drivername: 'my.jdbc.DriverName',
    // jvmOptions: '-Xrs',
    // defaultJars: ['derby.jar', 'derbyclient.jar', 'derbytools.jar']
  },
```

This adapter uses the [JDBC](https://github.com/CraZySacX/node-jdbc) module,
and most configuration options will be forwarded to that module.  The adapter
and URL are the only required fields, but it is common to specify the minpoolsize
and maxpoolsize as well.  Using user and password properties (rather than putting
them in the connection URL string) may work for you, but I've had better luck just
including them in the connection URL.

Although it is typically unnecessary, to use multiple JDBC drivers in tandem it may be
necessary to use the initializer (experimental):

```javascript
var jdbcInit = require('waterline-derby/initializer');
jdbcInit.initialize(['-Xrs'], ['derby.jar', 'derbyclient.jar', 'derbytools.jar']);
```

Check out **Connections** in the Sails docs, or see the `config/connections.js`
file in a new Sails project for information on setting up adapters.


### Run tests

You can set environment variables to override the default database config for the tests, e.g.:

```sh
$ APACHE_DERBY_WATERLINE_TEST_URL='jdbc:derby://localhost:1527/TEST;user=me;password=mine' npm test
```

Default settings are:

```javascript
{
  url: process.env.APACHE_DERBY_WATERLINE_TEST_URL || 'jdbc:derby://localhost:1527/TEST',
  minpoolsize: 10,
  maxpoolsize: 100,
  schema: true
}
```

### More Resources

- [Stackoverflow](http://stackoverflow.com/questions/tagged/sails.js)
- [#sailsjs on Freenode](http://webchat.freenode.net/) (IRC channel)
- [Twitter](https://twitter.com/sailsjs)
- [Professional/enterprise](https://github.com/balderdashy/sails-docs/blob/master/FAQ.md#are-there-professional-support-options)
- [Tutorials](https://github.com/balderdashy/sails-docs/blob/master/FAQ.md#where-do-i-get-help)
- <a href="http://sailsjs.org" target="_blank" title="Node.js framework for building realtime APIs."><img src="https://github-camo.global.ssl.fastly.net/9e49073459ed4e0e2687b80eaf515d87b0da4a6b/687474703a2f2f62616c64657264617368792e6769746875622e696f2f7361696c732f696d616765732f6c6f676f2e706e67" width=60 alt="Sails.js logo (small)"/></a>


### License

**[MIT](./LICENSE)**
&copy; 2016 [balderdashy](http://github.com/balderdashy) & [contributors]
[Mike McNeil](http://michaelmcneil.com), [Balderdash](http://balderdash.co) & contributors

[Sails](http://sailsjs.org) is free and open-source under the [MIT License](http://sails.mit-license.org/).

