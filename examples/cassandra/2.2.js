module.exports = function (client) {
    return new Promise(function(resolve, reject) {
        var query = 'create table foo (bar int primary key)';
        client.db.execute(query, function(err, results) {
            if (err) {
                console.log(err);
                return reject(new Error('Error applying migration '));
            }
            resolve(results);
        });
    });
};
