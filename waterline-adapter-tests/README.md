Waterline Adapter Tests
==========================
[![Build Status](https://travis-ci.org/balderdashy/waterline-adapter-tests.svg?branch=master)](https://travis-ci.org/balderdashy/waterline-adapter-tests)
[![npm version](https://badge.fury.io/js/waterline-adapter-tests.svg)](http://badge.fury.io/js/waterline-adapter-tests)
[![Dependency Status](https://david-dm.org/balderdashy/waterline-adapter-tests.svg)](https://david-dm.org/balderdashy/waterline-adapter-tests)

A set of integration tests that can be included in your Waterline Adapter module and used to test
your adapter against the current Waterline API.

## Adapter Interface Specification

+ [Reference](https://github.com/balderdashy/sails-docs/blob/master/contributing/adapter-specification.md)
+ [Philosophy & Motivations](https://github.com/balderdashy/sails-docs/blob/master/contributing/intro-to-custom-adapters.md)


## Usage

#### Write a test runner

> i.e. `runner.js`

```javascript
/**
 * Test runner dependencies
 */
var mocha = require('mocha');
var TestRunner = require('waterline-adapter-tests');


/**
 * Integration Test Runner
 *
 * Uses the `waterline-adapter-tests` module to
 * run mocha tests against the specified interfaces
 * of the currently-implemented Waterline adapter API.
 */
new TestRunner({

	// Load the adapter module.
	adapter: require('./relative/path/to/your/adapter'),

	// Default adapter config to use.
	config: {
		schema: false
	},

	// The set of adapter interfaces to test against.
	interfaces: ['semantic', 'queryable']
});
```

#### Run the tests

```sh
$ node runner.js
```


## Running Tests in a Vagrant VM

Since it is not necessarily desirable to install all the databases on the local host 
where this package is tested a [Vagrant](https://www.vagrantup.com) configuration for 
a fully configured virtual host is provided. To run the tests using this virtual host
follow [these steps](.puppet/README.md). 

Using Vagrant is entirely optional. If you prefer to just run the test on your host
directly just ensure the various databases being tested are installed.


## MIT License

See LICENSE.md.
