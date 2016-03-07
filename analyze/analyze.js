'use strict';

var DocumentClient = require('documentdb').DocumentClient,
    azureInfo = require('../secret/azureinfo.js'),
    Promise = require('promise'),
    _ = require('underscore');

var host = azureInfo.host,
    masterKey = azureInfo.masterKey,
    client = new DocumentClient(host, {masterKey: masterKey}),
    timeoutBetweenPulls = 1000, // 1s
    timeoutDelay = 5000; // 5s


// analyzeFaction('auren');
analyzeFactions();

function analyzeFactions() {
    console.log("analyze factions");
    analyzeFaction('auren')
    .then(x => analyzeFaction('witches'))
    .then(x => analyzeFaction('engineers'))
    .then(x => analyzeFaction('dwarves'))
    .then(x => analyzeFaction('mermaids'))
    .then(x => analyzeFaction('swarmlings'))
    .then(x => analyzeFaction('darklings'))
    .then(x => analyzeFaction('alchemists'))
    .then(x => analyzeFaction('halflings'))
    .then(x => analyzeFaction('cultists'))
    .then(x => analyzeFaction('nomads'))
    .then(x => analyzeFaction('fakirs'))
    .then(x => analyzeFaction('giants'))
    .then(x => analyzeFaction('chaosmagicians'))
    .then(x => console.log("done"))
    .catch(x => { console.log("failed"); console.log(x); console.log(x.stack); });
}

function analyzeFaction(faction) {
    return new Promise(function(resolve, reject) { 
        console.log("start download for: " + faction);
        getFactionGames(faction)
        .then(x => getGameData(x, faction))
        .then(x => analyzeGames(x, faction))
        .then(x => uploadFactionResults(x, faction))
        .then(x => { console.log(x); return x; })
        .then(resolve)
        .catch(resolve); // always keep uploading
    });
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
                .then(x => gameData.push(x))
                .catch(console.log);
            }, timeout, game);
        }
        setTimeout(() => resolve(gameData), i * timeoutBetweenPulls + timeoutDelay);
    });
}

function pullGame(game, faction) { 
    return new Promise(function(resolve, reject) { 
        var q = "select c." + faction + " from c where c.id = '" + game + "'";
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
            { // options
                type: 'manual', 
                buckets: [
                    { min: 0, max: 2, label: 'no points'}, // 0
                    { min: 3, max: 8, label: '3rd'}, // 3 and 6
                    { min: 9, max: 14, label: '2nd'}, // 9 and 12
                    { min: 15, max: 18, label: '1st'}, // 15 and 18
                ]
            }
        );
        obj.cult = createHistogram(
            _.map(gameData, x => x.simple.endGameCult), 
            {bucketsize: 4, type: 'auto', labels: 'range'});

        resolve(obj);
    });
}

// type = 'range' (e.g. '0-9') or 'exact' (e.g. '2')
function createHistogram(scores, options) { 
    // need to generate bucket names differently for type == 'range'
    if(options.type == 'auto') { 
        var counts = _.countBy(scores, x => { 
            if(isNaN(x)) { return 0; }
            else { return Math.floor(x / options.bucketsize) * options.bucketsize; }
        });
        var keys = _.keys(counts);
        var nums = _.map(keys, x => parseInt(x, 10));
        var ordered = _.sortBy(nums, x => x);
        if(options.labels == 'exact') { 
            return _.map(ordered, (x, i) => ({order: i, key: x, value: counts[x]}));
        } else if(options.labels == 'range') { 
            return _.map(ordered, (x, i) => ({
                order: i, 
                value: counts[x], 
                key: x + "-" + (x+options.bucketsize-1)
            }));
        }
    } else if(options.type == 'manual') {
        var counts = _.countBy(scores, x => { 
            if(isNaN(x)) { x = 0; } // is this legal?
            for(var i = 0; i < options.buckets.length; i++) {
                var b = options.buckets[i];
                if(x >= b.min && x <= b.max) { 
                    return b.label;
                }
            }
            return "uncategorized";
        });

        var ordering = { "uncategorized": options.buckets.length };
        _.each(options.buckets, (b,i) => ordering[b.label] = i);

        return  _.map(_.keys(counts), 
            x => ({ order: ordering[x], key: x, value: counts[x] })
        );

    } else { 
        throw "type must be 'auto' or 'manual'";
    }
}

// function makeHistogramForFactionPoints(gameData, )

function uploadFactionResults(data, faction) { 
    return new Promise(function(resolve, reject) { 
        var collLink = 'dbs/dev/colls/factions';
        client.upsertDocument(collLink, data, function(err, document) {
            if(err) { reject({ step: "upload", err: err }); }
            else { 
                resolve({ success: true, faction: faction })
            }
        });
    });
}




