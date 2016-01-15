var DocumentClient = require('documentdb').DocumentClient;
var azureInfo = require('./secret/azureinfo.js');

var host = azureInfo.host;
var masterKey = azureInfo.masterKey;


var client = new DocumentClient(host, {masterKey: masterKey});

var dbQuery = {query: "Select * from root"};
var dbOptions = {};
var db = '';
client.queryDatabases(dbQuery, dbOptions).toArray(function(err, results) {
    if(err) { 
        console.log("db lookup failed");
        console.log(err);
        return;
    }

    db = results[0];

    var cQuery = {query: "Select * from db"};
    var cOptions = {};
    var c = '';
    var dbLink = 'dbs/' + db.id;
    client.queryCollections(dbLink, cQuery, cOptions).toArray(function(err, results) {
        if(err) { 
            console.log("collection lookup failed");
            console.log(err);
            return;
        }

        c = results[0];

        var dQuery = {query: "Select * from c"};
        var dOptions = {};
        var d = '';
        var collLink = dbLink + '/colls/' + c.id;
        console.log(collLink);
        client.queryDocuments(collLink, dQuery, dOptions).toArray(function(err, results) {
            if(err) { 
                console.log("document lookup failed");
                console.log(err);
                return;
            }

            d = results[0];

            console.log(d.name);
        });

        // var doc = {hello:'world'};
        // client.createDocument(collLink, doc, function(err, document) {
        //     if(err) { 
        //         console.log("document addition failed");
        //         console.log(err);
        //         return;
        //     }

        //     console.log('created document: ' + JSON.stringify(document));
        // });

    });
});





