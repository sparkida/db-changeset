module.exports = function (client) {
    return new Promise(function(resolve, reject) {
        var query = 'select count(1) from test.users';
        client.db.execute(query, function(err, results) {
            if (err) {
                return reject(new Error('Error applying migration '));
            }
            resolve(results);
        });
    });
};
