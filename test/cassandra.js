const changeset = require('../bin/changeset');
const dbConfig = require('../config.sample.js');
const assert = require('assert');

let client;
let admin;
let config = {
    debug: false,
    verbose: false,
    dbConfig: dbConfig.cassandra,
    name: 'cassandra',
    changesetDirectory: 'examples/cassandra'
};
let connectionGenerator = function*(done, handler, skipConnect) {
    let db = config.dbConfig.keyspace;
    if (!skipConnect) {
        yield admin.connect().then(handler).catch(handler);
    }
    yield admin.db.execute(`drop keyspace if exists ${db}`, handler);
    yield admin.db.execute(`create keyspace ${db} WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};`, done);
};

before(() => {
    let c = JSON.parse(JSON.stringify(config));
    c.dbConfig.keyspace = 'system';
    admin = changeset.getClient(c);
});

beforeEach((done) => {
    let gen = connectionGenerator(done, (err) => {
        if (err) {
            return done(err);
        }
        gen.next();
    }, !!admin.db);
    gen.next();
});


afterEach((done) => {
    client.db.shutdown(done);
});

after((done) => {
    admin.db.shutdown(done);
});

describe('Cassandra', () => {
    it ('should update current schema version to target cql file', (done) => {
        let c = Object.assign({updateFile: '1.1.cql'}, config);
        client = changeset.getClient(c);
        client.runScript().then(() => {
            client.db.execute('select file_id,schema_version from schema_version', (err, result) => {
                let rows = result.rows;
                assert(rows, 'no rows returned');
                assert.equal(rows.length, 1);
                assert.equal(rows[0].file_id, 1);
                assert.equal(rows[0].schema_version, 1);
                done();
            });
        }).catch(done);
    });
    it ('should update current schema version to target js file', (done) => {
        let c = Object.assign({updateFile: '2.2.js'}, config);
        client = changeset.getClient(c);
        client.runScript().then(() => {
            client.db.execute('select file_id,schema_version from schema_version', (err, result) => {
                let rows = result.rows;
                assert(rows, 'no rows returned');
                assert.equal(rows.length, 1);
                assert.equal(rows[0].file_id, 2);
                assert.equal(rows[0].schema_version, 2);
                done();
            });
        }).catch(done);
    });
    it ('should target a changeset and update schema from that point', (done) => {
        let c = Object.assign({targetFile: '2.2.js'}, config);
        client = changeset.getClient(c);
        client.runScript().then(() => {
            client.db.execute('select file_id,schema_version from schema_version', (err, result) => {
                let rows = result.rows;
                assert(rows, 'no rows returned');
                assert.equal(rows.length, 2);
                assert.equal(rows[0].file_id, 3);
                assert.equal(rows[0].schema_version, 2);
                done();
            });
        }).catch(done);
    });
    it ('should run all changesets', (done) => {
        client = changeset.getClient(config);
        client.runScript().then(() => {
            client.db.execute('select file_id,schema_version from schema_version', (err, result) => {
                let rows = result.rows;
                assert(rows, 'no rows returned');
                assert.equal(rows.length, 3);
                assert.equal(rows[0].file_id, 3);
                assert.equal(rows[0].schema_version, 2);
                done();
            });
        }).catch(done);
    });
});
