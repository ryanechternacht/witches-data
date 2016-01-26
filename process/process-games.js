var parser = require('./parse.js'),
    rulesEngine = require('./rules_engine.js'),
    DocumentClient = require('documentdb').DocumentClient;
    azureInfo = require('../secret/azureinfo.js'),
    Promise = require('promise'),
    _ = require('underscore');

var host = azureInfo.host,
    masterKey = azureInfo.masterKey,
    gamename = 'jan30';

var client = new DocumentClient(host, {masterKey: masterKey});

var q = {query: "Select c.ledger from c where c.id = '" + gamename + "'"};
var o = {};
var collLink = 'dbs/snellman/colls/games';
client.queryDocuments(collLink, q, o).toArray(function(err, results) {
    if(err) { 
        console.log("document lookup failed");
        console.log(err);
        return;
    }

    // excerpt from online query explorer
    // {
    //   "ledger": [
    //     {
    //       "comment": " Game jan30\r"
    //     },
    //     {
    //       "comment": " List players (in any order) with 'player' command\r"
    //     },
    // ...

    // console.log(results[0].ledger[100]);

    var ledger = results[0].ledger;

    var parsedLog = parser.parseLog(ledger);

    var engineSetup = rulesEngine.setupEngine(parsedLog, ledger);

    var scoreCards = rulesEngine.processCommands(engineSetup, parsedLog, ledger);
    var players = _.sortBy(scoreCards, 'total').reverse();

    var gameComplete = rulesEngine.checkGameComplete(parsedLog);

    var doc = {
        factions: players,
        rounds: engineSetup.rounds,
        fireAndIceBonus: engineSetup.fireAndIceBonus,
        gameComplete: gameComplete,
        id: gamename
    };

    var collLink = 'dbs/dev/colls/games';
    client.createDocument(collLink, doc, function(err, document) {
        if(err) { 
            console.log("document addition failed");
            console.log(err);
            return;
        }

        // where to log?
        console.log('created document: ' + doc.id);
    });
});
