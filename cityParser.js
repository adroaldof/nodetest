var fs = require('fs'),
    _ = require('underscore'),
    City = require('./cityModel'),
    filePath = '',
    citiesJSON = {},
    mongoose = require('mongoose'),
    reader = require ('buffered-reader'),
    Join = require('join'),
    states = {
        'Acre': {'abbreviation': 'AC'},
        'Alagoas': {'abbreviation': 'AL'},
        'Amazonas': {'abbreviation': 'AM'},
        'Amapá': {'abbreviation': 'AP'},
        'Bahia': {'abbreviation': 'BA'},
        'Ceará': {'abbreviation': 'CE'},
        'Distrito Federal': {'abbreviation': 'DF'},
        'Espírito Santo': {'abbreviation': 'ES'},
        'Goiás': {'abbreviation': 'GO'},
        'Maranhão': {'abbreviation': 'MA'},
        'Minas Gerais': {'abbreviation': 'MG'},
        'Mato Grosso do Sul': {'abbreviation': 'MS'},
        'Mato Grosso': {'abbreviation': 'MT'},
        'Pará': {'abbreviation': 'PA'},
        'Paraíba': {'abbreviation': 'PB'},
        'Pernambuco': {'abbreviation': 'PE'},
        'Piauí': {'abbreviation': 'PI'},
        'Paraná': {'abbreviation': 'PR'},
        'Rio de Janeiro': {'abbreviation': 'RJ'},
        'Rio Grande do Norte': {'abbreviation': 'RN'},
        'Rondônia': {'abbreviation': 'RO'},
        'Roraima': {'abbreviation': 'RR'},
        'Rio Grande do Sul': {'abbreviation': 'RS'},
        'Santa Catarina': {'abbreviation': 'SC'},
        'Sergipe': {'abbreviation': 'SE'},
        'São Paulo': {'abbreviation': 'SP'},
        'Tocantins': {'abbreviation': 'TO'}
    };

filePath = process.argv[2];

var pushToMongo = function (data) {
    var join = Join.create();

    mongoose.connect('mongodb://localhost/City');

    _.each(data, function (state) {
        City.saveToMongo({_id:state._id, cities:state.cities, name:state.name}, join.add());
    });

    join.when(function () {
        mongoose.disconnect();
    });
};

var generateJson = function (data) {
    var line = data.split('\t'),
        state = line[1],
        abbr = states[state].abbreviation;

    citiesJSON[abbr] = citiesJSON[abbr] ? citiesJSON[abbr] : {};
    citiesJSON[abbr]['cities'] = citiesJSON[abbr]['cities'] ? citiesJSON[abbr]['cities'] : [];

    var city = {
        '_id': String(line[0]) + String(line[2]),
        'name': String(line[3].split('\r')[0])
    };

    if (_.isNumber(parseInt(line[0]))) {
        if (!_.findWhere(citiesJSON[abbr], city)) {
            citiesJSON[abbr]['cities'].push(city);
        }
    }

    if (!_.findWhere(citiesJSON[abbr]['name'] === state)) {
        citiesJSON[abbr]['name'] = state;
    }

    if (!_.findWhere(citiesJSON[abbr]['_id'] === abbr)) {
        citiesJSON[abbr]['_id'] = abbr;
    }
};

var readLines = function (input, callback) {
    var remaining = '';

    input.on('data', function (data) {
        remaining += data;
        var index =  remaining.indexOf('\n'),
            last = 0;

        while (index > -1) {
            var line = remaining.substring(0, index);
            remaining = remaining.substring(index + 1);
            if (line.toLowerCase().indexOf('uf') < 0) {
                callback(line);
            }
            index =  remaining.indexOf('\n', last);
        }

        remaining = remaining.substring(last);
    });

    input.on('error', function (err) {
        console.log('Error: ' + err);
    })


    input.on('end', function () {
        if (remaining.length > 0) {
            callback(remaining);
        }
        pushToMongo(citiesJSON);
    });
};

var input = fs.createReadStream(filePath);
readLines(input, generateJson);
