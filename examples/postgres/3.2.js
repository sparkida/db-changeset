module.exports = (client) => {
    let transaction = client.beginTransaction();
    return new Promise((resolve, reject) => {
        transaction.add('create table test3(name text, age int);');
        transaction.add('create table test4(name text, age int);');
        transaction.commit().then(resolve).catch(reject);
    });
};
