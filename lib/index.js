// Generated by CoffeeScript 1.7.1

/*
  backbone-mongo.js 0.6.3
  Copyright (c) 2013 Vidigami - https://github.com/vidigami/backbone-mongo
  License: MIT (http://www.opensource.org/licenses/mit-license.php)
 */

(function() {
  var Backbone, BackboneMongo, BackboneORM, key, publish, value, _, _ref, _ref1;

  _ref = BackboneORM = require('backbone-orm'), _ = _ref._, Backbone = _ref.Backbone;

  module.exports = BackboneMongo = require('./core');

  publish = {
    configure: require('./lib/configure'),
    sync: require('./sync'),
    _: _,
    Backbone: Backbone
  };

  _.extend(BackboneMongo, publish);

  BackboneMongo.modules = {
    'backbone-orm': BackboneORM
  };

  _ref1 = BackboneORM.modules;
  for (key in _ref1) {
    value = _ref1[key];
    BackboneMongo.modules[key] = value;
  }

}).call(this);
