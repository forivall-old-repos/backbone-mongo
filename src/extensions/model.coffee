###
  backbone-mongo.js 0.6.0
  Copyright (c) 2013-2014 Vidigami
  License: MIT (http://www.opensource.org/licenses/mit-license.php)
  Source: https://github.com/vidigami/backbone-orm
  Dependencies: Backbone.js and Underscore.js.
###

{_, CacheCursor} = require 'backbone-orm'
modelEach = require './model_each'

module.exports = (model_type) ->

  cache_cursor = is_cache = null
  modelEachCache = (query, iterator, callback) ->
    cache_cursor or= model_type.cursor()
    is_cache or= cache_cursor instanceof CacheCursor

    return modelEach(cursorFactory, query, iterator, callback) unless is_cache

    options = query.$each or {}
    return modelEach(bypassCursorFactory, query, iterator, callback) if options.cache is false or options.json
    origEach(query, iterator, callback)

  bypassCursorFactory = (query) -> cache_cursor.wrapped_sync_fn('cursor', parsed_query)
  cursorFactory = (query) -> model_type.cursor(parsed_query)

  origEach = model_type.each
  model_type.each = (query, iterator, callback) ->
    [query, iterator, callback] = [{}, query, iterator] if arguments.length is 2
    modelEachCache(query, iterator, callback)

  model_type.eachC = (query, callback, iterator) ->
    [query, callback, iterator] = [{}, query, callback] if arguments.length is 2
    modelEachCache(model_type, query, iterator, callback)
