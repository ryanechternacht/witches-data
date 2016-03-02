'use strict';

var DocumentClient = require('documentdb').DocumentClient,
    azureInfo = require('../secret/azureinfo.js'),
    Promise = require('promise'),
    _ = require('underscore');

var host = azureInfo.host,
    masterKey = azureInfo.masterKey,
    client = new DocumentClient(host, {masterKey: masterKey}),
    timeoutBetweenPulls = 2000, // 2s
    timeoutDelay = 2000; // 5s


analyzeFaction('auren');


function analyzeFaction(faction) { 
    getFactionGames(faction)
    .then(x => getGameData(x, faction))
    .then(x => analyzeGames(x, faction))
    // .then(uploadFactionResults)
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

        obj.total = createHistogram(
            _.map(gameData, x => x.total), 
            {bucketsize: 10, type: 'auto', labels: 'range'}
        );
        obj.network = createHistogram(
            _.map(gameData, x => x.simple.endGameNetwork), 
            {bucketsize: 6, type: 'auto', labels: 'exact'}
        );

        resolve(obj);
    });
}

// type = 'range' (e.g. '0-9') or 'exact' (e.g. '2')
function createHistogram(scores, options) { 
    // need to generate bucket names differently for type == 'range'
    if(options.type == 'auto') { 
        if(options.labels == 'exact') { 
            var counts = _.countBy(scores, x => { 
                if(isNaN(x)) { return 0; }
                else { return Math.floor(x / options.bucketsize) * options.bucketsize; }
            });
            var keys = _.keys(counts);
            return _.map(keys, x => { var obj = {}; obj[x] = counts[x]; return obj; } );
            // return _.toArray(_.map(keys, x => {x: counts[x]}));
        } else if(options.labels == 'range') { 
            var counts = _.countBy(scores, x => { 
                if(isNaN(x)) { return 0; }
                else { 
                    var n = Math.floor(x / options.bucketsize) * options.bucketsize; 
                    return n + "-" + (n+options.bucketsize-1);
                }
            });
            var keys = _.keys(counts);
            return _.map(keys, x => { var obj = {}; obj[x] = counts[x]; return obj; } );
        }
    } else if(options.type == 'manual') {

    } else { 
        throw "type must be 'auto' or 'manual'";
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




