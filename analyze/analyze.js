var DocumentClient = require('documentdb').DocumentClient;
    azureInfo = require('../secret/azureinfo.js'),
    Promise = require('promise'),
    _ = require('underscore');

var host = azureInfo.host,
    masterKey = azureInfo.masterKey,
    client = new DocumentClient(host, {masterKey: masterKey});


analyzeFaction('dwarves');


function analyzeFaction(faction) { 
    getFactionGames(faction)
    .then(analyzeGames)
    .then(console.log)
    .catch(console.log);
}

function getFactionGames(faction) { 
    return new Promise(function(resolve, reject) { 
        var q = "SELECT * FROM c IN games.factions where c.faction = '" + faction + "'";
        var o = {};
        var coll = 'dbs/dev/colls/games';
        client.queryDocuments(coll, q, o).toArray(function(err, results) { 
            if(err) { 
                reject("couldn't get games");
            }

            resolve(results);
        });
    });
}

function analyzeGames(games) { 
    // return avg(_.map(games, function(g) { return g.total }));

    var obj = {};
    var keys = _.keys(games[0].detailed); // get keys from every object
    for(var i = 0; i < keys.length; i++) { 
        var k = keys[i];
        var arr = []; 

        var values = _.map(games, function(g) { return g.detailed[k] || 0; });

        arr.mean = avg(values);
        arr.med = median(values);

        obj[k] = arr;
    }
    return obj;
}

function avg(array) { 
    var sum = 0;
    for(var i = 0; i < array.length; i++) { 
        sum += array[i];
    }
    return sum / array.length;
}

function median(array) { // this is too lazy
    var ordered = _.sortBy(array, function(x) { return x });
    return ordered[Math.floor(array.length/2)];
}




