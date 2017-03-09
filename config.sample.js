module.exports = {
    cassandra: {
        contactPoints: ['vm.vertebrae.io'],
        protocolOptions: {
            port: 9042
        },
        keyspace: 'tester',
        dialect: 'cassandra'
    },
    postgres: {
        host: 'vm.vertebrae.io',
        db: 'schematest',
        username: 'postgres',
        password: 'postgres',
        dialect: 'postgres'
    }
};
