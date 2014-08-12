###
  backbone-mongo.js 0.6.0
  Copyright (c) 2013-2014 Vidigami
  License: MIT (http://www.opensource.org/licenses/mit-license.php)
  Source: https://github.com/vidigami/backbone-orm
  Dependencies: Backbone.js and Underscore.js.
###

modelEach = require './model_each'
MongoSync = require '../sync'

module.exports = (model_type) ->
  return unless model_type::sync('sync') instanceof MongoSync

  model_type.each = (query, iterator, callback) ->
    [query, iterator, callback] = [{}, query, iterator] if arguments.length is 2
    modelEach(model_type, query, iterator, callback)

  model_type.eachC = (query, callback, iterator) ->
    [query, callback, iterator] = [{}, query, callback] if arguments.length is 2
    modelEach(model_type, query, iterator, callback)
