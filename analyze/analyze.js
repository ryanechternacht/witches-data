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


// var q = "select * from c where c.id = '" + 'auren' + "'";
// var collLink = 'dbs/dev/colls/factions';
// client.queryDocuments(collLink, q).toArray(function(err, results) { 
//     if(err) { console.log(err); }
//     else { console.log(results); }
// });

// var q = "select * from c where c.id = '4pLeague_S10_D2L2_G1'";
// var collLink = 'dbs/dev/colls/games';
// client.queryDocuments(collLink, q).toArray(function(err, results) { 
//     if(err) { console.log(err); }
//     else { console.log(results); }
// });

// analyzeAllFactions()
// .then(console.log)
// .catch(x => { console.log("failed"); console.log(x); console.log(x.stack); });


analyzeFactions();
// analyzeFaction("fakirs");

// getSampleGames()
// .then(getGameDataForAllFactions)
// .then(x => analyzeAllGames(x))
// .then(x => uploadFactionResults(x, "all"))
// .then(x => console.log("done"))
// .catch(x => console.log(x.stack)); // always keep uploading


function analyzeFactions() {
    console.log("analyze factions");
    Promise.resolve() 
    .then(x => analyzeFaction('fakirs'))
    .then(x => analyzeFaction('nomads'))
    .then(x => analyzeFaction('auren'))
    .then(x => analyzeFaction('witches'))
    .then(x => analyzeFaction('engineers'))
    .then(x => analyzeFaction('dwarves'))
    .then(x => analyzeFaction('mermaids'))
    .then(x => analyzeFaction('swarmlings'))
    .then(x => analyzeFaction('darklings'))
    .then(x => analyzeFaction('alchemists'))
    .then(x => analyzeFaction('halflings'))
    .then(x => analyzeFaction('cultists'))
    .then(x => analyzeFaction('giants'))
    .then(x => analyzeFaction('chaosmagicians'))
    .then(x => analyzeAllFactions())
    .then(console.log)
    .catch(x => { console.log("failed"); console.log(x); console.log(x.stack); });
}

function analyzeFaction(faction) {
    console.log(faction);

    return new Promise(function(resolve, reject) { 
        getFactionGames(faction)
        .then(x => getGameData(x, faction))
        .then(x => analyzeGames(x, faction))
        .then(x => uploadFactionResults(x, faction))
        .then(resolve)
        .catch(x => { console.log(x); resolve(x); }); // always keep uploading
    });
}

function analyzeAllFactions() {
    console.log("all");

    return new Promise(function(resolve, reject) { 
        console.log("start download for: all factions");
        getAllGames()
        .then(getGameDataForAllFactions)
        .then(x => analyzeAllGames(x))
        .then(x => uploadFactionResults(x, "all"))
        .then(resolve)
        .catch(x => { console.log(x); resolve(x); }); // always keep uploading
    });
}

function getFactionGames(faction) { 
    return new Promise(function(resolve, reject) { 
        var q = "SELECT c.id from c where array_contains(c.factions, '" + faction + "')";
        var collLink = 'dbs/dev/colls/games';
        client.queryDocuments(collLink, q).toArray(function(err, results) { 
            if(err) { 
                reject({ step: "pull games", err: err });
            }
            resolve(results);
        });
    });
}

function getAllGames() { 
    return new Promise(function(resolve, reject) { 
        var q = "SELECT c.id, c.factions from c";
        var collLink = 'dbs/dev/colls/games';
        client.queryDocuments(collLink, q).toArray(function(err, results) { 
            if(err) { 
                reject({ step: "pull games", err: err });
            }
            resolve(results);
        });
    });
}

function getSampleGames() {
    return new Promise(function(resolve, reject) { 
        resolve([
            {id: "4pLeague_S6_D1L1_G1", factions: ["darklings", "nomads", "dwarves", "witches"]},
            {id: "4pLeague_S6_D1L1_G2", factions: ["darklings", "nomads", "mermaids", "engineers"]},
            {id: "4pLeague_S6_D1L1_G3", factions: ["halflings", "chaosmagicians", "mermaids", "witches"]},
            {id: "4pLeague_S6_D1L1_G4", factions: ["darklings", "nomads", "swarmlings", "witches"]},
            {id: "4pLeague_S6_D1L1_G5", factions: ["darklings", "engineers", "halflings", "giants"]},
            {id: "4pLeague_S6_D1L1_G6", factions: ["darklings", "nomads", "mermaids", "engineers"]},
        ]);
    });
}

// gameList is { id }[]
function getGameData(gameList, faction) { 
    return new Promise(function(resolve, reject) { 
        var i = 0; 
        var gameData = [];
        for(/* i */; i < gameList.length; i++) { 
            let game = gameList[i];
            let j = i;
            var timeout = i * timeoutBetweenPulls;
            setTimeout(() => { 
                pullGame(game.id, faction)
                .then(x => gameData.push(x))
                .then(x => console.log(game, j))
                .catch(console.log);
            }, timeout, game);
        }
        setTimeout(() => resolve(gameData), i * timeoutBetweenPulls + timeoutDelay);
    });
}

// gamelist is { id, factions[] }[]
function getGameDataForAllFactions(gameList) {
    return new Promise(function(resolve, reject) { 
        var gameData = [];
        var count = 0; 
        for(var i = 0; i < gameList.length; i++) { 
            let game = gameList[i];
            for(var j = 0; j < game.factions.length; j++) {
                let faction = j;
                var timeout = count * timeoutBetweenPulls;
                count++;
                let current = count;

                //TODO change below to grab each faction from game
                setTimeout(() => { 
                    pullGame(game.id, game.factions[faction])
                    .then(x => gameData.push(x))
                    .then(x => console.log(game.id, game.factions[faction], current))
                    .catch(console.log);
                }, timeout, game);
            }
        }
        setTimeout(() => resolve(gameData), count * timeoutBetweenPulls + timeoutDelay);
    });
}

function pullGame(game, faction) { 
    return new Promise(function(resolve, reject) { 
        var q = "select c." + faction + ", c.results from c where c.id = '" + game + "'";
        var collLink = 'dbs/dev/colls/games';
        client.queryDocuments(collLink, q).toArray(function(err, results) { 
            if(err) { reject({ step: "pull game", err: err, game: game }); }
            else { 
                var d = results[0];
                resolve({game: d[faction], results: d.results }); 
            }
        });
    });
}

function analyzeGames(gameData, faction) { 
    return new Promise(function(resolve, reject) { 
        var obj = { id: faction, faction: faction };

        obj.total = createHistogram(
            _.map(gameData, x => x.game.total), 
            {bucketsize: 10, type: 'auto', labels: 'decades'}
        );
        obj.network = createHistogram(
            _.map(gameData, x => x.game.simple.endGameNetwork), 
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
        obj.buildings = createHistogram(
            _.map(gameData, x => x.game.d + x.game.tp + x.game.te + x.game.sh + x.game.sa), 
            { bucketsize: 1, type: 'auto', labels: 'exact'}
        );
        obj.cult = createHistogram(
            _.map(gameData, x => x.game.simple.endGameCult), 
            {bucketsize: 4, type: 'auto', labels: 'range'});
        obj.games = gameData.length;
        obj.favors = createFavorsHistogram(_.map(gameData, x => x.game.favors));
        // obj.pickOrder = createHistogram(
        //     _.map(gameData, x => x.game.startOrder), // startOrder is 0-indexed 
        //     {bucketsize: 1, type: 'auto', labels: 'exact'}
        // );
        obj.pickOrder = createHistogram(
            _.map(gameData, x => x.game.startOrder), 
            { // options
                type: 'manual', 
                buckets: [
                    { min: 1, max: 1, label: '1st'},
                    { min: 2, max: 2, label: '2nd'},
                    { min: 3, max: 3, label: '3rd'},
                    { min: 4, max: 4, label: '4th'},
                ]
            }
        );
        obj.results = createHistogram(
            _.map(gameData, x => {
                var result = _.find(x.results, y => y.faction == faction);
                return result.place;
            }),
            { // options
                type: 'manual', 
                buckets: [
                    { min: 1, max: 1, label: '1st'},
                    { min: 2, max: 2, label: '2nd'},
                    { min: 3, max: 3, label: '3rd'},
                    { min: 4, max: 4, label: '4th'},
                ]
            }
        );

        resolve(obj);
    });
}

function analyzeAllGames(gameData) { 
    return new Promise(function(resolve, reject) { 
        var obj = { id: "all", faction: "all" };

        obj.total = createHistogram(
            _.map(gameData, x => x.game.total), 
            {bucketsize: 10, type: 'auto', labels: 'decades'}
        );
        obj.network = createHistogram(
            _.map(gameData, x => x.game.simple.endGameNetwork), 
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
        obj.buildings = createHistogram(
            _.map(gameData, x => x.game.d + x.game.tp + x.game.te + x.game.sh + x.game.sa), 
            { bucketsize: 1, type: 'auto', labels: 'exact'}
        );
        obj.cult = createHistogram(
            _.map(gameData, x => x.game.simple.endGameCult), 
            {bucketsize: 4, type: 'auto', labels: 'range'});
        obj.favors = createFavorsHistogram(_.map(gameData, x => x.game.favors));
        
        obj.results = createFactionComparisons(gameData);

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
        } else if(options.labels == 'decades') { 
            return _.map(ordered, (x, i) => ({
                order: i,
                value: counts[x],
                key: x + "'s"
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

function createFavorsHistogram(favors) { 
    //[{key,order,value}]
    var favs = ["fav1", "fav2", "fav3", "fav4", "fav5", "fav6", 
                "fav7", "fav8", "fav9", "fav10", "fav11", "fav12"];

    var obj = [];

    for(var i = 0; i < favs.length; i++) { 
        var fav = favs[i];

        var count = _.filter(favors, x => _.includes(x, fav)).length;

        obj.push({ key: fav, order: i, value: count });
    }

    return obj;
}

function createFactionComparisons(gameData) {
    var createResult = x => ({
        faction: x, 
        fakirs: {win: 0, tie: 0, loss: 0}, nomads: {win: 0, tie: 0, loss: 0}, 
        auren: {win: 0, tie: 0, loss: 0}, witches: {win: 0, tie: 0, loss: 0}, 
        engineers: {win: 0, tie: 0, loss: 0}, dwarves: {win: 0, tie: 0, loss: 0}, 
        mermaids: {win: 0, tie: 0, loss: 0}, swarmlings: {win: 0, tie: 0, loss: 0}, 
        darklings: {win: 0, tie: 0, loss: 0}, alchemists: {win: 0, tie: 0, loss: 0}, 
        halflings: {win: 0, tie: 0, loss: 0}, cultists: {win: 0, tie: 0, loss: 0}, 
        giants: {win: 0, tie: 0, loss: 0}, chaosmagicians: {win: 0, tie: 0, loss: 0}
    });

    var resultTable = [
        createResult("dwarves"),
        createResult("engineers"),
        createResult("chaosmagicians"),
        createResult("giants"),
        createResult("fakirs"),
        createResult("nomads"),
        createResult("halflings"),
        createResult("cultists"),
        createResult("alchemists"),
        createResult("darklings"),
        createResult("swarmlings"),
        createResult("mermaids"),
        createResult("auren"),
        createResult("witches"),
    ];

    // we only need to grab the result set for each game once, however each game
    // occurs in the dataset 4 times, once for each faction. we thus skip every 
    // 4 records (since each game is stored consecutively).
    for(var g = 0; g < gameData.length; g+=4) { 
        var results = gameData[g].results;

        for(var r = 0; r < results.length; r++) { 
            var result = results[r];
            var faction = _.find(resultTable, x => x.faction == result.faction);

            // loop the results again, to compare every result to each other
            // this could be optimized by doing bi-directional comparisons, but whatever
            for(var o = 0; o < results.length; o++) {
                if(o == r) { // skip yourself
                    continue;
                }

                var other = results[o];

                if(result.place < other.place) { // win
                    faction[other.faction].win = faction[other.faction].win + 1;
                } else if(result.place > other.place) { // loss
                    faction[other.faction].loss = faction[other.faction].loss + 1;
                } else { // tie
                    faction[other.faction].tie = faction[other.faction].tie + 1;
                }
            }
        }
    }

    return resultTable;
}

function uploadFactionResults(data, faction) { 
    return new Promise(function(resolve, reject) { 
        var collLink = 'dbs/dev/colls/factions';
        client.upsertDocument(collLink, data, function(err, document) {
            if(err) { reject({ step: "upload", err: err }); }
            else { 
                resolve({ success: true, faction: faction });
            }
        });
    });
}




