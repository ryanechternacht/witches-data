'use strict';

var DocumentClient = require('documentdb').DocumentClient,
    azureInfo = require('../secret/azureinfo.js'),
    Promise = require('promise'),
    _ = require('underscore');

var host = azureInfo.host,
    masterKey = azureInfo.masterKey,
    client = new DocumentClient(host, {masterKey: masterKey}),
    timeoutBetweenPulls = 2000, // 2s
    timeoutDelay = 5000; // 5s


analyzeFaction('auren');


function analyzeFaction(faction) { 
    getFactionGames(faction)
    .then(x => getGameData(x, faction))
    .then(x => analyzeGames(x, faction))
    .then(uploadFactionResults)
    .then(console.dir)
    .catch(console.log);
}

function getFactionGames(faction) { 
    return new Promise(function(resolve, reject) { 
        var q = "SELECT c.id from c where array_contains(c.factions, '" + faction + "')"
        var collLink = 'dbs/dev/colls/games';
        client.queryDocuments(collLink, q).toArray(function(err, results) { 
            if(err) { 
                reject({ step: "pull games", err: err });
            }
            resolve(results);
        });
    });
}

function getGameData(gameList, faction) { 
    return new Promise(function(resolve, reject) { 
        var i = 0; 
        var gameData = [];
        for(/* i */; i < gameList.length; i++) { 
            let game = gameList[i];
            var timeout = i * timeoutBetweenPulls;
            setTimeout(() => { 
                pullGame(game.id, faction)
                .then(x => { gameData.push(x); console.log("pulled: " + game.id); })
                .catch(console.log);
            }, timeout, game);
        }
        setTimeout(() => { resolve(gameData) }, i * timeoutBetweenPulls + timeoutDelay);
    });
}

function pullGame(game, faction) { 
    return new Promise(function(resolve, reject) { 
        var q = "select c." + faction + " from c where c.id = '" + game + "'";
        console.log(q);
        var collLink = 'dbs/dev/colls/games';
        client.queryDocuments(collLink, q).toArray(function(err, results) { 
            if(err) { reject({ step: "pull game", err: err, game: game }); }
            else { resolve(results[0][faction]); }
        });
    });
}

function analyzeGames(gameData, faction) { 
    return new Promise(function(resolve, reject) { 
        var obj = { id: faction, faction: faction };

        obj.total = createHistogram(_.map(gameData, x => x.total), 10, 'range');
        obj.network = createHistogram(_.map(gameData, x => x.simple.endGameNetwork), 6, 'exact');

        resolve(obj);
    });
}

// type = 'range' (e.g. '0-9') or 'exact' (e.g. '2')
function createHistogram(scores, bucketsize, type) { 
    // need to generate bucket names differently for type == 'range'
    if(type == 'exact') { 
        return _.countBy(scores, x => { 
            if(isNaN(x)) { return 0; }
            else { return Math.floor(x / bucketsize) * bucketsize; }
        });
    } else if(type == 'range') { 
        return _.countBy(scores, x => { 
            if(isNaN(x)) { return 0; }
            else { 
                var n = Math.floor(x / bucketsize) * bucketsize; 
                return n + "-" + (n+bucketsize-1);
            }
        });
    } else { 
        throw "type must be 'range' or 'exact'";
    }
}

function uploadFactionResults(data) { 
    return new Promise(function(resolve, reject) { 
        var collLink = 'dbs/dev/colls/factions';
        client.upsertDocument(collLink, data, function(err, document) {
            if(err) { reject({ step: "upload", err: err }); }
            else { 
                resolve({ success: true })
            }
        });
    });
}
