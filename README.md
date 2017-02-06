Cassandra Changesets
====================

NodeJS module for running changesets against databases


Current Support DB Drivers
==========================

- Postgres
- Cassandra

How To Run
==========

```bash
node ./bin/changeset-cli.js config.js
```

Configuration
=============

```js
module.exports = {
    contactPoints: ['vm.vertebrae.io'],
    protocolOptions: {
        port: 9042
    },
    keyspace: 'test'
};
```

Changesets
==========

The changeset files should all reside at the root level of the directory
specified by `-d, --directory` cli option or the `config.changesetDirectory` property. Each configuration file should
follow the naming convention `<VERSION>.cql` or `<VERSION>.js`

Javascript Changeset Files
==========================
.js changeset files are javascript modules that return a function with one argument, a reference to the changeset instance. When invoked this function returns a promise, generator, or array of promises/generators.  

Example:
```javascript
module.exports = function(changeset){
    return new Promise(function(resolve, reject) {
        var query = 'select count(1) from test.users';
        changeset.client.execute(query, function(err, results) {
            if (err) {
                return reject(new Error('Error applying changeset '));
            }
            resolve(results);
        });
    });
};
```
CQL Changeset Files
===================

***Note:*** Use three hyphens (**---**) for multiple CQL statements

Example:
```
CREATE TABLE example_table (
  id int PRIMARY KEY,
  date timestamp
);
---
CREATE TABLE example_table_2 (
  name text,
  email text
);
```
