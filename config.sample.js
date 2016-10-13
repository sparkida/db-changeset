module.exports = {
    cassandra: {
        contactPoints: ['vm.vertebrae.io'],
        protocolOptions: {
            port: 9042
        },
        keyspace: 'test',
        dialect: 'cassandra'
    },
    postgres: {
        host: 'vm.vertebrae.io',
        db: 'test',
        username: 'postgres',
        password: 'postgres',
        dialect: 'postgres'
    }
};
