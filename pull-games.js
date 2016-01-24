var DocumentClient = require('documentdb').DocumentClient,
    azureInfo = require('./secret/azureinfo.js'),
    argv = require('minimist')(process.argv.slice(2)),
    _ = require('underscore'),
    moment = require('moment'),
    // http = require('http'),
    // Promise = require('promise'),
    httpSync = require('http-sync'),
    fs = require('fs'),
    sleep = require('sleep');

if(argv['?'] || argv.h || _.contains(argv._, 'help')) { 
    console.log('\t-a \t\t start at first game on snellman');
    console.log('\t-d date \t (YYYY-MM-DD) start at date on snellman');
    console.log('\t-r \t\t resume from file set in -f, uses default if none set');
    console.log('\t-s \t\t start a new run even if a temp file exists');
    console.log('\t-f file \t store progress in file');
    console.log('\t-h, -?, help \t print help');
    return;
}

var defaults = setupDefaults(),
    tempFile = setupTempFile(argv, defaults),
    today = moment(),
    host = azureInfo.host,
    masterKey = azureInfo.masterKey;

var date, gameList;
if(argv['r']) { 
    if(argv['d'] || argv['a']) { 
        console.log("Can't use -d, -a, or -s with -r. Exiting...");
        return;
    }
    if(!fs.existsSync(tempFile)) {
        console.log("Temp file [" + tempFile + "] not found. Exiting...");
        return;
    }

    var load = loadStatusFileSync(tempFile);
    date = load.date;
    gameList = load.gameList;
} else 
{
    // unless -s or no tmp file
    if(!(argv['s'] || !fs.existsSync(tempFile))) { 
        console.log('Last run is not finished. Either resume (with -r) or pass -s to start a new run. Exiting...');
        return;
    }
    date = setupDate(argv, defaults);
    var lookup = lookupDateSync(date);
    gameList = lookup.gameList;
    // do something with lookup.players
}

while(date < today) { 
    // if empty, get new games
    if(gameList.length == 0) { 
        // incrememnt day
        date = date.add(1, 'day');
        console.log(date.format());

        // if we're already up to date, quit
        if(date >= today) { 
            fs.unlinkSync(tempFile);
            break;
        }

        var lookup = lookupDateSync(date);
        gameList = lookup.gameList;
        // do something with lookup.players
    }

    var game = gameList.shift();
        // ledger = pullGameSync(game);
    // do something with game
    // console.log(ledger[100]);
    console.log(game);

    writeStatusSync(tempFile, date, gameList);

    sleep.usleep(1000000); // 1s
    // sleep.sleep(30); // 30s

}


// SUPPORTING FUNCTIONS
function setupDefaults() {
    return {
        tempFile: 'run.tmp',
        startDate: moment('2013-05-01', 'YYYY-MM-DD')
    };
}

function setupTempFile(argv, defaults) { 
    if(argv.f) { 
        if(typeof argv.f == "string") { 
            return argv.f;
        } else {
            throw new Error('BAD ARGUMENTS: -f must pass a file');
        }
    }
    else { 
        return defaults.tempFile;
    }
}

function setupDate(argv, defaults) { 
    if(argv.d) { 
        if(typeof argv.d == "string") { 
            return moment(argv.d, 'YYYY-MM-DD');
        } else {
            throw new Error('BAD ARGUMENTS: -d muss pass a date');
        }
    }
    else {
        return defaults.startDate;
    }
}

function loadStatusFileSync(tempFile) { 
    var file = fs.readFileSync(tempFile, 'utf-8'),
        data = file.split('\n'),
        date = moment(data.shift());

    return { 
        date: date,
        gameList: data
    };
}

function lookupDateSync(date) { 
    var lookup = pullDateSync(date),
        gameList = lookup.games,
        players = lookup.players;

    return {
        gameList: gameList,
        players: players
    };
}

// returns {player, games}
function pullDateSync(date) { 
    var path = '/app/results/v2/' + date.format('YYYY/MM/DD');
    var options = {
        host: 'terra.snellman.net',
        port: 80,
        path: path,
        method: 'GET'
    };

    var request = httpSync.request(options);
    var timeout = false;
    request.setTimeout(10000, function() {
        timeout = true;
    });
    var response = request.end(); // execute synchronously
    
    if(timeout) {
        throw new Error("lookupDateSync timed out");
    }
    var data = JSON.parse(response.body.toString());
    return {
        players: data.players,
        games: _.keys(data.games)
    }
}

function writeStatusSync(tempFile, date, gameList) { 
    var data = date.format();
    for(var i = 0; i < gameList.length; i++) { 
        var game = gameList[i];
        data += '\n' + game;
    }

    fs.writeFileSync(tempFile, data);
}

function pullGameSync(game) { 
    var path = '/app/view-game/?game=' + game;
    var options = {
        host: 'terra.snellman.net',
        port: 80,
        path: path,
        method: 'GET'
    };

    var request = httpSync.request(options);
    var timeout = false;
    request.setTimeout(10000, function() {
        timeout = true;
    });
    var response = request.end(); // execute synchronously
    
    if(timeout) {
        throw new Error("pullGameSync timed out");
    }
    var data = JSON.parse(response.body.toString());
    return data.ledger;
}



// var client = new DocumentClient(host, {masterKey: masterKey});

// var dQuery = {query: "Select * from c"};
// var dOptions = {};
// var d = '';
// var collLink = 'dbs/snellman-raw/colls/games'
// client.queryDocuments(collLink, dQuery, dOptions).toArray(function(err, results) {
//     if(err) { 
//         console.log("document lookup failed");
//         console.log(err);
//         return;
//     }

//     d = results[0];

//     console.log(d.name);
// });



