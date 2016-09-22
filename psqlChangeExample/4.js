module.exports = (client) => {
    return new Promise((resolve, reject) => {
        var query = 'select * from schema_version';
        client.runQuery(query)
            .then((results) => {
                console.log(results);
                resolve();
            }, (err) => {
                reject(new Error('Error!', err));
            });
    });
};
