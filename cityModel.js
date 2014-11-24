'use strict';

var mongoose = require('mongoose'),
    Schema = mongoose.Schema,

    CitySchema = new Schema({
        _id: String,
        cities: [{name: String, _id: String}],
        name: String
    });


var City = mongoose.model('City', CitySchema);

var saveToMongo = function(json, callback) {
    var cities = new City(json);
    cities.save(function (err) {
        if (err) {
            console.log('Mongo error: ' + err);
        }

        callback();
    })
}

module.exports = {
    'saveToMongo': saveToMongo
};
